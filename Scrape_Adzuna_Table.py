import pyodbc
import asyncio
import signal
import os
import argparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from asyncio import Semaphore, Queue
import random
import time
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.getenv("DbConnectionAzure")

if not CONNECTION_STRING:
    raise RuntimeError("DbConnectionAzure not set in environment (.env missing or not loaded)")

UPDATE_SUCCESS_QUERY = "UPDATE JobPosts SET FullDescription = ?, Scrape = 1 WHERE Id = ?"

DEFAULT_CONCURRENT_LIMIT = 8
DEFAULT_RETRY_LIMIT = 2
DEFAULT_RETRY_DELAY = 2
DEFAULT_BATCH_SIZE = 100
DEFAULT_CHUNK_SIZE = 500
DEFAULT_DAYS_BACK = 14

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

BLOCKED_RESOURCE_TYPES = ["image", "stylesheet", "font", "media", "xhr"]
BLOCKED_URL_KEYWORDS = ["googlesyndication", "doubleclick", "adservice", "analytics", "facebook", "adnxs"]

shutdown_requested = False

# Global configuration variables (will be set by parse_args)
CONCURRENT_LIMIT = DEFAULT_CONCURRENT_LIMIT
RETRY_LIMIT = DEFAULT_RETRY_LIMIT
RETRY_DELAY = DEFAULT_RETRY_DELAY
BATCH_SIZE = DEFAULT_BATCH_SIZE
CHUNK_SIZE = DEFAULT_CHUNK_SIZE
DAYS_BACK = DEFAULT_DAYS_BACK

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Scrape job descriptions from Adzuna')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                       help=f'Batch size for database operations (default: {DEFAULT_BATCH_SIZE})')
    parser.add_argument('--days-back', type=int, default=DEFAULT_DAYS_BACK,
                       help=f'Number of days back to fetch jobs (default: {DEFAULT_DAYS_BACK})')
    parser.add_argument('--concurrent-limit', type=int, default=DEFAULT_CONCURRENT_LIMIT,
                       help=f'Number of concurrent scraping tasks (default: {DEFAULT_CONCURRENT_LIMIT})')
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE,
                       help=f'Size of job chunks to process (default: {DEFAULT_CHUNK_SIZE})')
    parser.add_argument('--retry-limit', type=int, default=DEFAULT_RETRY_LIMIT,
                       help=f'Number of retry attempts per job (default: {DEFAULT_RETRY_LIMIT})')
    parser.add_argument('--retry-delay', type=int, default=DEFAULT_RETRY_DELAY,
                       help=f'Delay between retries in seconds (default: {DEFAULT_RETRY_DELAY})')
    
    return parser.parse_args()

class DatabaseManager:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self._connection = None
        self._lock = asyncio.Lock()
    
    async def get_connection(self):
        async with self._lock:
            if self._connection:
                try:
                    cursor = self._connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                except:
                    try:
                        self._connection.close()
                    except:
                        pass
                    self._connection = None
            
            if self._connection is None:
                self._connection = pyodbc.connect(self.connection_string)
                print("-> Connected to Azure SQL Database")
            return self._connection
    
    async def get_date_range_info(self):
        """Get information about the date range of jobs to be scraped"""
        try:
            conn = await self.get_connection()
            cursor = conn.cursor()
            
            date_info_query = f"""
                SELECT 
                    MIN(Created) as OldestJob,
                    MAX(Created) as NewestJob,
                    COUNT(*) as TotalJobs
                FROM JobPosts
                WHERE FullDescription IS NULL 
                AND IsDetailsUrl = 1
                AND Scrape IS NULL
                AND Created >= DATEADD(day, -{DAYS_BACK}, GETDATE())
            """
            
            cursor.execute(date_info_query)
            result = cursor.fetchone()
            cursor.close()
            
            return {
                'oldest': result[0],
                'newest': result[1], 
                'total': result[2]
            }
        except Exception as e:
            print(f"-> Error getting date range info: {e}")
            return None
    
    async def execute_batch(self, operations):
        """Execute batch operations with optimized performance"""
        if not operations:
            return True
            
        try:
            conn = await self.get_connection()
            cursor = conn.cursor()
            
            cursor.fast_executemany = True
            
            success_ops = [(op['desc'], op['job_id']) for op in operations if op['type'] == 'success']
            
            if success_ops:
                cursor.executemany(UPDATE_SUCCESS_QUERY, success_ops)
            
            conn.commit()
            cursor.close()
            return True
            
        except Exception as e:
            print(f"-> Database operation failed: {e}")
            raise
    
    async def close(self):
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None

async def close_popups(page):
    """Quick popup closing"""
    popup_texts = ["Accept", "Got it", "Reject All", "Accept All"]
    for text in popup_texts:
        try:
            await page.click(f"button:has-text('{text}')", timeout=1000)
        except:
            pass

async def fetch_full_description(page, job_url, job_id):
    """Optimized scraping function"""
    try:
        await page.goto(job_url, timeout=40000, wait_until="domcontentloaded")
        
        await close_popups(page)
        
        primary_selectors = [
            "section.adp-body",
            "[class*='adp-body']",
            ".job-description",
            "#description"
        ]
        
        for selector in primary_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    text = await element.inner_text()
                    if text and len(text.strip()) > 100:
                        return text.strip()
            except:
                continue
        
        try:
            body_text = await page.evaluate("document.body.innerText")
            if body_text and len(body_text.strip()) > 200:
                return body_text.strip()
        except:
            pass
            
        return None
        
    except Exception as e:
        print(f"-> [{job_id}] Error: {e}")
        return None

async def intercept_route(route, request):
    """Block unnecessary resources"""
    if (
        request.resource_type in BLOCKED_RESOURCE_TYPES or
        any(domain in request.url for domain in BLOCKED_URL_KEYWORDS)
    ):
        await route.abort()
    else:
        await route.continue_()

async def batch_writer(queue: Queue, db_manager: DatabaseManager):
    """Enhanced batch writer with graceful shutdown support"""
    batch = []
    last_commit_time = time.time()
    
    while True:
        try:
            if shutdown_requested and queue.empty():
                break
                
            item = await asyncio.wait_for(queue.get(), timeout=30.0)
            if item == "DONE":
                break
            
            batch.append(item)
            
            current_time = time.time()
            should_commit = (
                len(batch) >= BATCH_SIZE or 
                (current_time - last_commit_time) > 60 or
                shutdown_requested 
            )
            
            if should_commit:
                await db_manager.execute_batch(batch)
                print(f"-> Batch committed: {len(batch)} jobs")
                batch.clear()
                last_commit_time = current_time
                
            queue.task_done()
            
        except asyncio.TimeoutError:
            if batch:
                await db_manager.execute_batch(batch)
                print(f"-> Timeout batch committed: {len(batch)} jobs")
                batch.clear()
                last_commit_time = time.time()
                
            if shutdown_requested:
                break
    
    if batch:
        await db_manager.execute_batch(batch)
        print(f"-> Final batch committed: {len(batch)} jobs")

async def scrape_job(semaphore, browser, job, result_queue: Queue):
    """Optimized single job scraping with shutdown check"""
    if shutdown_requested:
        return
        
    async with semaphore:
        job_data = job

        if len(job_data) == 3:
            job_id, url, created = job_data
        else:
            job_id, url = job_data
            
        user_agent = random.choice(USER_AGENTS)
        
        context = await browser.new_context(
            user_agent=user_agent,
            viewport={'width': 1920, 'height': 1080},
            java_script_enabled=True
        )
        
        page = await context.new_page()
        await page.route("**/*", intercept_route)
        
        for attempt in range(RETRY_LIMIT):
            if shutdown_requested:
                break
                
            try:
                full_desc = await fetch_full_description(page, url, job_id)
                
                if full_desc:
                    await result_queue.put({
                        'type': 'success',
                        'job_id': job_id,
                        'desc': full_desc
                    })
                    print(f"-> Id: {job_id}")
                    break
                else:
                    if attempt == RETRY_LIMIT - 1:
                        await result_queue.put({
                            'type': 'fail',
                            'job_id': job_id
                        })
                        print(f"-> Id: {job_id}")
                    else:
                        await asyncio.sleep(RETRY_DELAY)
                        
            except Exception as e:
                print(f"-> Id: {job_id} attempt {attempt + 1} failed: {e}")
                if attempt == RETRY_LIMIT - 1:
                    await result_queue.put({
                        'type': 'fail',
                        'job_id': job_id
                    })
                    print(f"-> Id: {job_id} - Final failure")
                else:
                    await asyncio.sleep(RETRY_DELAY)
        
        await page.close()
        await context.close()

async def process_chunk(browser, jobs_chunk, result_queue: Queue):
    """Process a chunk of jobs concurrently with shutdown support"""
    semaphore = Semaphore(CONCURRENT_LIMIT)
    tasks = []
    
    for job in jobs_chunk:
        if shutdown_requested:
            break
            
        task = asyncio.create_task(
            scrape_job(semaphore, browser, job, result_queue)
        )
        tasks.append(task)
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"-> Task {i} failed with exception: {result}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print(f"\n-> Shutdown signal received ({signum}). Finishing current batch and saving progress...")
    shutdown_requested = True

async def main():
    global shutdown_requested, CONCURRENT_LIMIT, RETRY_LIMIT, RETRY_DELAY, BATCH_SIZE, CHUNK_SIZE, DAYS_BACK
    
    # Parse command line arguments
    args = parse_args()
    
    # Update global variables with command line arguments
    CONCURRENT_LIMIT = args.concurrent_limit
    RETRY_LIMIT = args.retry_limit
    RETRY_DELAY = args.retry_delay
    BATCH_SIZE = args.batch_size
    CHUNK_SIZE = args.chunk_size
    DAYS_BACK = args.days_back
    
    print(f"-> Configuration:")
    print(f"   - Days back: {DAYS_BACK}")
    print(f"   - Batch size: {BATCH_SIZE}")
    print(f"   - Concurrent limit: {CONCURRENT_LIMIT}")
    print(f"   - Chunk size: {CHUNK_SIZE}")
    print(f"   - Retry limit: {RETRY_LIMIT}")
    print(f"   - Retry delay: {RETRY_DELAY}s")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    start_time = time.time()
    db_manager = DatabaseManager(CONNECTION_STRING)

    try:
        print("-> Connecting to database...")
        date_info = await db_manager.get_date_range_info()
        if date_info:
            print(f"-> Date range: {date_info['oldest']} to {date_info['newest']}")
        
        FETCH_ALL_QUERY = f"""
            SELECT Id, Url, Created
            FROM JobPosts
            WHERE FullDescription IS NULL 
            AND IsDetailsUrl = 1 
            AND Scrape IS NULL
            AND Created >= DATEADD(day, -{DAYS_BACK}, GETDATE())
            ORDER BY Created DESC, Id
        """
        
        conn = await db_manager.get_connection()
        cursor = conn.cursor()
        print(f"-> Executing query to fetch jobs...")
        cursor.execute(FETCH_ALL_QUERY)
        all_jobs = cursor.fetchall()
        total_jobs = len(all_jobs)
        cursor.close()

        if total_jobs == 0:
            print("-> No fresh jobs to scrape.")
            return

        print(f"-> Total jobs to scrape: {total_jobs}")
        result_queue = Queue()
        writer_task = asyncio.create_task(batch_writer(result_queue, db_manager))

        print("-> Starting browser...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            processed = 0
            chunk_num = 0

            for i in range(0, total_jobs, CHUNK_SIZE):
                if shutdown_requested:
                    break

                jobs_chunk = all_jobs[i:i + CHUNK_SIZE]
                chunk_num += 1
                chunk_start = time.time()
                print(f"-> Processing chunk {chunk_num} ({len(jobs_chunk)} jobs)")

                await process_chunk(browser, jobs_chunk, result_queue)

                processed += len(jobs_chunk)
                chunk_time = time.time() - chunk_start
                elapsed_time = time.time() - start_time
                speed = processed / elapsed_time if elapsed_time > 0 else 0
                eta = (total_jobs - processed) / speed if speed > 0 else 0

                print(f"-> Progress: {processed}/{total_jobs} ({processed/total_jobs*100:.1f}%) - "
                      f"Speed: {speed:.1f} jobs/sec - ETA: {eta/60:.1f} min - Chunk time: {chunk_time:.1f}s")

            await browser.close()

        await result_queue.put("DONE")
        await writer_task

        if shutdown_requested:
            print("-> Shutdown completed. All progress saved.")
    except Exception as e:
        print(f"-> Main execution error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db_manager.close()

    total_time = time.time() - start_time
    if shutdown_requested:
        print(f"->  Interrupted after {total_time:.1f} seconds.")
    else:
        print(f"-> Completed {total_jobs} fresh jobs in {total_time:.1f} seconds ({total_jobs/total_time:.1f} jobs/sec)")

if __name__ == "__main__":
    asyncio.run(main())