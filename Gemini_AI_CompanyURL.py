import os
import pyodbc
from google import genai
from google.genai import types
import json
import textwrap
import time
from typing import List, Tuple, Optional
from contextlib import contextmanager
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.getenv("DbConnectionAzure")

if not CONNECTION_STRING:
    raise RuntimeError("DbConnectionAzure not set in environment (.env missing or not loaded)")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY not set in environment")

client_gemini = genai.Client(api_key=GEMINI_API_KEY)

class DatabaseManager:
    """Enhanced database manager with connection pooling and retry logic."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.max_retries = 3
        self.retry_delay = 2
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections with automatic retry."""
        conn = None
        for attempt in range(self.max_retries):
            try:
                conn = pyodbc.connect(
                    self.connection_string,
                    timeout=30,  # Connection timeout
                    autocommit=False
                )
                # Set connection properties for better stability
                conn.execute("SET LOCK_TIMEOUT 30000")  # 30 seconds lock timeout
                yield conn
                return
                
            except Exception as e:
                print(f"-> Database connection attempt {attempt + 1} failed: {e}")
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt)) 
                else:
                    raise Exception(f"Failed to establish database connection after {self.max_retries} attempts")
    
    def execute_with_retry(self, query: str, params: tuple = (), fetch_method: str = None):
        """Execute query with retry logic and proper error handling."""
        for attempt in range(self.max_retries):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    
                    if fetch_method == 'fetchall':
                        result = cursor.fetchall()
                    elif fetch_method == 'fetchone':
                        result = cursor.fetchone()
                    elif fetch_method == 'identity':
                        cursor.execute("SELECT @@IDENTITY")
                        result = cursor.fetchone()[0]
                    else:
                        result = None
                    
                    conn.commit()
                    return result
                    
            except Exception as e:
                print(f"-> Query execution attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise e

# Initialize database manager
db_manager = DatabaseManager(CONNECTION_STRING)

def send_to_gemini(prompt: str, temperature: float = 0.0, top_p: float = 0.1, top_k: int = 1) -> Optional[str]:
    """Send prompt to Gemini API with error handling and retry logic."""
    config = types.GenerateContentConfig(
        temperature=temperature,
        top_p=top_p,
        top_k=top_k
    )
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client_gemini.models.generate_content(
                model="gemini-2.0-flash",
                config=config,
                contents=prompt
            )
            
            if (response and 
                hasattr(response, 'candidates') and 
                response.candidates and 
                len(response.candidates) > 0 and
                hasattr(response.candidates[0], 'content') and
                response.candidates[0].content and
                hasattr(response.candidates[0].content, 'parts') and
                response.candidates[0].content.parts and
                len(response.candidates[0].content.parts) > 0 and
                hasattr(response.candidates[0].content.parts[0], 'text')):
                
                text_response = response.candidates[0].content.parts[0].text
                if text_response:
                    return text_response.strip()
            
            print(f"->  Gemini API returned empty or malformed response on attempt {attempt + 1}")
            return None
            
        except Exception as e:
            print(f"->  Gemini API attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None

def validate_url(url: str) -> Optional[str]:
    """Validate and normalize URL format."""
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip()
    if not url:
        return None
    
    # Remove common unwanted text
    unwanted_phrases = [
        "not available", "n/a", "unknown", "none", "null", 
        "no website", "no url", "not found", "unavailable"
    ]
    
    url_lower = url.lower()
    if any(phrase in url_lower for phrase in unwanted_phrases):
        return None
    
    # Add https:// if no protocol specified
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return None
        
        # Basic domain validation
        domain = parsed.netloc.lower()
        if '.' not in domain or len(domain) < 4:
            return None
        
        # Remove trailing slashes and normalize
        normalized_url = f"{parsed.scheme}://{parsed.netloc}"
        if parsed.path and parsed.path != '/':
            normalized_url += parsed.path.rstrip('/')
        
        return normalized_url
        
    except Exception:
        return None

def find_company_url(company_name: str, country_name: str, country_code: str) -> Optional[str]:
    """Use Gemini AI to find the official website URL for a company."""
    
    if not company_name or not company_name.strip():
        return None
    
    # Clean company name
    company_name = company_name.strip()
    
    prompt = textwrap.dedent(f"""\
        CRITICAL: You MUST return ONLY valid JSON. No explanations, no additional text.
        
        Find the official website URL for this company and return EXACTLY this JSON format:
        {{"url": "https://example.com"}}
        
        MANDATORY RULES:
        - Return ONLY the main official company website URL
        - Use HTTPS protocol when possible
        - Return ONLY the domain (e.g., https://company.com, not https://company.com/about)
        - If company has multiple domains, return the PRIMARY/MAIN one
        - If you cannot find a reliable official website, return: {{"url": null}}
        - Do NOT return social media URLs (LinkedIn, Facebook, Twitter, etc.)
        - Do NOT return job board URLs or third-party listings
        - Do NOT return subsidiary or product-specific URLs unless it's the main company site
        - Return ONLY the JSON object, nothing else
        
        Company Information:
        - Company Name: "{company_name}"
        - Country: {country_name} ({country_code})
        
        Examples of CORRECT responses:
        {{"url": "https://microsoft.com"}}
        {{"url": "https://apple.com"}}
        {{"url": "https://volkswagen.com"}}
        {{"url": null}}
    """)
    
    try:
        response_text = send_to_gemini(prompt)
        
        if not response_text:
            print(f"-> No response from Gemini API for company: {company_name}")
            return None
        
        # Clean response text
        generated_text = response_text
        if generated_text.startswith("```json"):
            generated_text = generated_text[7:].rstrip("```").strip()
        elif generated_text.startswith("```"):
            generated_text = generated_text[3:].rstrip("```").strip()
        
        # Extract JSON
        start_idx = generated_text.find('{')
        end_idx = generated_text.rfind('}')
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            generated_text = generated_text[start_idx:end_idx+1]
        
        if not generated_text.strip():
            print(f"-> Empty response after cleaning for company: {company_name}")
            return None
        
        # Parse JSON
        try:
            data = json.loads(generated_text)
        except json.JSONDecodeError as e:
            print(f"-> JSON decode error for company {company_name}: {e}")
            print(f"   Raw response: {response_text[:200]}...")
            return None
        
        # Extract URL
        if isinstance(data, dict):
            url = data.get("url")
            if url:
                return validate_url(url)
        
        return None
        
    except Exception as e:
        print(f"-> Unexpected error finding URL for company {company_name}: {e}")
        return None

def fetch_companies_without_url(batch_size: int = 50) -> List[Tuple[int, str, str, str]]:
    """Fetch companies that don't have URLs, with country information."""
    try:
        return db_manager.execute_with_retry(f"""
            SELECT TOP {batch_size} 
                c.Id, 
                c.CompanyName, 
                co.CountryName, 
                co.CountryCode
            FROM Companies c
            INNER JOIN Countries co ON c.CountryId = co.Id
            WHERE c.Url IS NULL OR c.Url = ''
            ORDER BY c.Id
        """, fetch_method='fetchall')
        
    except Exception as e:
        print(f"-> Database fetch error: {e}")
        return []

def update_company_url(company_id: int, url: str) -> bool:
    """Update the URL for a company in the database."""
    try:
        db_manager.execute_with_retry(
            "UPDATE Companies SET Url = ? WHERE Id = ?",
            (url, company_id)
        )
        return True
    except Exception as e:
        print(f"-> Error updating URL for company {company_id}: {e}")
        return False

def mark_company_url_not_found(company_id: int) -> bool:
    """Mark company as having no URL found by setting a special value."""
    try:
        # Set URL to 'NOT_FOUND' to avoid reprocessing
        db_manager.execute_with_retry(
            "UPDATE Companies SET Url = 'NOT_FOUND' WHERE Id = ?",
            (company_id,)
        )
        return True
    except Exception as e:
        print(f"-> Error marking company {company_id} as not found: {e}")
        return False

def process_company_urls(batch_size: int = 50, max_companies: int = 1000):
    """Process companies without URLs and find their official websites."""
    try:
        # Count total companies to process
        total_companies_result = db_manager.execute_with_retry("""
            SELECT COUNT(*) FROM Companies c
            INNER JOIN Countries co ON c.CountryId = co.Id
            WHERE c.Url IS NULL OR c.Url = ''
        """, fetch_method='fetchone')
        
        total_companies = total_companies_result[0]
        print(f"-> Total companies without URLs: {total_companies}")
        
        if max_companies > 0:
            print(f"-> Processing maximum {max_companies} companies")
            total_companies = min(total_companies, max_companies)
        
        processed_count = 0
        found_count = 0
        not_found_count = 0
        failed_count = 0
        
        # Process in batches
        while processed_count < total_companies:
            remaining = min(batch_size, total_companies - processed_count, max_companies - processed_count)
            if remaining <= 0:
                break
                
            companies_batch = fetch_companies_without_url(remaining)
            
            if not companies_batch:
                print("-> No more companies to process")
                break
            
            print(f"-> Processing batch of {len(companies_batch)} companies...")
            
            for company_id, company_name, country_name, country_code in companies_batch:
                try:
                    print(f"-> Finding URL for: {company_name} ({country_name})")
                    
                    url = find_company_url(company_name, country_name, country_code)
                    
                    if url:
                        if update_company_url(company_id, url):
                            found_count += 1
                            print(f"   ✓ Found URL: {url}")
                        else:
                            failed_count += 1
                            print(f"   ✗ Failed to update URL in database")
                    else:
                        if mark_company_url_not_found(company_id):
                            not_found_count += 1
                            print(f"   - No URL found")
                        else:
                            failed_count += 1
                            print(f"   ✗ Failed to mark as not found")
                    
                    processed_count += 1
                    
                    # Rate limiting to avoid API overload
                    time.sleep(1.0)
                    
                    # Show progress every 10 companies
                    if processed_count % 10 == 0:
                        print(f"-> Progress: {processed_count}/{total_companies} | Found: {found_count} | Not Found: {not_found_count} | Failed: {failed_count}")
                    
                    # Stop if we've reached the maximum
                    if max_companies > 0 and processed_count >= max_companies:
                        break
                        
                except Exception as e:
                    print(f"-> Error processing company {company_id} ({company_name}): {e}")
                    failed_count += 1
                    processed_count += 1
            
            # Brief pause between batches
            time.sleep(2.0)
            
            # Stop if we've reached the maximum
            if max_companies > 0 and processed_count >= max_companies:
                break
    
    except Exception as e:
        print(f"-> Critical error: {e}")
    
    except KeyboardInterrupt:
        print(f"\n-> Process interrupted. Total processed: {processed_count}")
    
    finally:
        print(f"-> Processing complete.")
        print(f"   Total processed: {processed_count}")
        print(f"   URLs found: {found_count}")
        print(f"   Not found: {not_found_count}")
        print(f"   Failed: {failed_count}")

def cleanup_not_found_markers():
    """Clean up 'NOT_FOUND' markers if you want to reprocess those companies."""
    try:
        result = db_manager.execute_with_retry(
            "UPDATE Companies SET Url = NULL WHERE Url = 'NOT_FOUND'",
            fetch_method='fetchone'
        )
        print("-> Cleaned up 'NOT_FOUND' markers. Companies can be reprocessed.")
    except Exception as e:
        print(f"-> Error cleaning up markers: {e}")

if __name__ == "__main__":
    process_company_urls(batch_size=20, max_companies=2000)
