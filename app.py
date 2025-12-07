from flask import Flask, jsonify, request
from flask_restx import Api, Resource, fields, Namespace
import threading
import subprocess
import sys
import os
import time
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict
import queue
import signal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configure Flask-RESTX
app.config['RESTX_MASK_SWAGGER'] = False
app.config['RESTX_VALIDATE'] = True
api = Api(app,
    version='1.0',
    title='Adzuna Job Processing API',
    description='API for managing Adzuna job scraping and AI processing tasks',
    doc='/docs/',
    prefix='/api/v1'
)

# Create namespaces
scraper_ns = Namespace('scraper', description='Web scraping operations')
ai_ns = Namespace('ai', description='AI processing operations')
company_ns = Namespace('company', description='Company URL processing operations')
coordinates_ns = Namespace('coordinates', description='Google Maps coordinates processing operations')
status_ns = Namespace('status', description='System status and monitoring')

api.add_namespace(scraper_ns)
api.add_namespace(ai_ns)
api.add_namespace(company_ns)
api.add_namespace(coordinates_ns)
api.add_namespace(status_ns)

# Global variables for task management
running_tasks: Dict[str, dict] = {}
task_queue = queue.Queue()
task_counter = 0

# Models for Swagger documentation
scraper_config_model = api.model('ScraperConfig', {
    'batch_size': fields.Integer(description='Number of jobs to process in each batch', default=50),
    'days_back': fields.Integer(description='Number of days back to fetch jobs', default=14),
    'concurrent_limit': fields.Integer(description='Number of concurrent scraping tasks', default=2),
    'chunk_size': fields.Integer(description='Size of job chunks to process', default=200),
    'retry_limit': fields.Integer(description='Number of retry attempts per job', default=2),
    'retry_delay': fields.Integer(description='Delay between retries in seconds', default=2)
})

ai_config_model = api.model('AIConfig', {
    'batch_size': fields.Integer(description='Number of jobs to process in each batch', default=100),
    'days_back': fields.Integer(description='Number of days back to process jobs', default=14)
})

company_url_config_model = api.model('CompanyURLConfig', {
    'batch_size': fields.Integer(description='Number of companies to process in each batch', default=20),
    'max_companies': fields.Integer(description='Maximum number of companies to process (0 for unlimited)', default=100),
    'cleanup_markers': fields.Boolean(description='Clean up NOT_FOUND markers before processing', default=False)
})

coordinates_config_model = api.model('CoordinatesConfig', {
    'batch_size': fields.Integer(description='Number of locations to process in each batch', default=50),
    'delay': fields.Float(description='Delay between API requests in seconds', default=0.2)
})

task_status_model = api.model('TaskStatus', {
    'task_id': fields.String(description='Unique task identifier'),
    'status': fields.String(description='Task status', enum=['running', 'completed', 'failed', 'cancelled', 'initializing']),
    'script': fields.String(description='Script being executed'),
    'start_time': fields.DateTime(description='Task start time'),
    'end_time': fields.DateTime(description='Task end time'),
    'created_time': fields.DateTime(description='Task creation time'),
    'progress': fields.String(description='Current progress information'),
    'logs': fields.List(fields.String, description='Task execution logs'),
    'error': fields.String(description='Error message if task failed'),
    'return_code': fields.Integer(description='Script return code')
})

def find_script(script_name: str) -> Path:
    """Find script in various possible locations"""
    script_dir = Path(__file__).parent
    possible_locations = [
        script_dir / script_name,
        script_dir / 'Adzuna' / script_name,
        script_dir.parent / script_name,
        script_dir.parent / 'Adzuna' / script_name,
    ]
    
    for location in possible_locations:
        if location.exists():
            logger.info(f"Found script at: {location}")
            return location
    
    raise FileNotFoundError(f"Script {script_name} not found in any of the expected locations: {possible_locations}")

def run_script_async(task_id: str, script_name: str, args: list = None):
    """Run a script asynchronously with better error handling and output capture"""
    global running_tasks
    
    if args is None:
        args = []
    
    try:
        # Update task status
        if task_id in running_tasks:
            running_tasks[task_id].update({
                'status': 'running',
                'start_time': datetime.now(),
                'logs': [],
                'progress': 'Script starting...'
            })
        
        # Find the script
        script_path = find_script(script_name)
        script_dir = script_path.parent
        
        # Prepare command with better Python execution
        cmd = [sys.executable, '-u', str(script_path)] + args  # -u for unbuffered output
        
        logger.info(f"Starting task {task_id}: {' '.join(cmd)}")
        logger.info(f"Working directory: {script_dir}")
        
        # Create environment with Python unbuffered output
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        env['PYTHONIOENCODING'] = 'utf-8'
        
        # Run the script with better configuration
        process = subprocess.Popen(
            cmd,
            cwd=str(script_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=0,  # Unbuffered
            env=env,
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None  # Create process group for better signal handling
        )
        
        # Store process for potential cancellation
        if task_id in running_tasks:
            running_tasks[task_id]['process'] = process
            running_tasks[task_id]['progress'] = 'Process started, waiting for output...'
        
        # Read output in real-time with timeout handling
        logs = []
        last_output_time = time.time()
        startup_timeout = 60  # 60 seconds for script startup
        
        while True:
            try:
                # Use select on Unix systems for better I/O handling
                if hasattr(os, 'select'):
                    import select
                    ready, _, _ = select.select([process.stdout], [], [], 1.0)
                    if ready:
                        output = process.stdout.readline()
                    else:
                        output = ''
                else:
                    output = process.stdout.readline()
                
                current_time = time.time()
                
                # Check if process is still alive
                if output == '' and process.poll() is not None:
                    break
                
                if output:
                    last_output_time = current_time
                    log_line = f"{datetime.now().strftime('%H:%M:%S')} - {output.strip()}"
                    logs.append(log_line)
                    
                    if task_id in running_tasks:
                        running_tasks[task_id]['logs'] = logs[-100:]  # Keep last 100 lines
                        running_tasks[task_id]['progress'] = output.strip()
                    
                    logger.info(f"Task {task_id}: {output.strip()}")
                
                # Check for startup timeout only in the beginning
                elif (current_time - last_output_time) > startup_timeout and len(logs) == 0:
                    logger.warning(f"Task {task_id}: No output received within {startup_timeout} seconds")
                    if task_id in running_tasks:
                        running_tasks[task_id]['progress'] = f"Warning: No output for {startup_timeout} seconds, but process is still running..."
                    startup_timeout = 300  # Extend timeout after first warning
                
                # Brief sleep to prevent high CPU usage
                if not output:
                    time.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error reading output for task {task_id}: {e}")
                break
        
        return_code = process.wait()
        
        if task_id in running_tasks:
            final_status = 'completed' if return_code == 0 else 'failed'
            error_msg = None if return_code == 0 else f"Script exited with code {return_code}"
            
            running_tasks[task_id].update({
                'status': final_status,
                'end_time': datetime.now(),
                'return_code': return_code,
                'error': error_msg,
                'progress': f"Script completed with return code: {return_code}"
            })
        
        logger.info(f"Task {task_id} completed with return code: {return_code}")
        
    except FileNotFoundError as e:
        logger.error(f"Task {task_id} failed - script not found: {str(e)}")
        if task_id in running_tasks:
            running_tasks[task_id].update({
                'status': 'failed',
                'end_time': datetime.now(),
                'error': f"Script not found: {str(e)}",
                'progress': f"Script not found: {str(e)}"
            })
    
    except Exception as e:
        logger.error(f"Task {task_id} failed: {str(e)}")
        if task_id in running_tasks:
            running_tasks[task_id].update({
                'status': 'failed',
                'end_time': datetime.now(),
                'error': str(e),
                'progress': f"Task failed: {str(e)}"
            })
    
    finally:
        # Clean up process reference
        if task_id in running_tasks and 'process' in running_tasks[task_id]:
            try:
                # Ensure process is terminated
                if 'process' in running_tasks[task_id]:
                    proc = running_tasks[task_id]['process']
                    if proc.poll() is None:
                        proc.terminate()
                        time.sleep(2)
                        if proc.poll() is None:
                            proc.kill()
            except:
                pass
            del running_tasks[task_id]['process']

def generate_task_id() -> str:
    """Generate a unique task ID"""
    global task_counter
    task_counter += 1
    return f"task_{int(time.time())}_{task_counter}"

@scraper_ns.route('/start')
class StartScraper(Resource):
    @scraper_ns.expect(scraper_config_model, validate=False)
    @scraper_ns.marshal_with(task_status_model)
    @scraper_ns.doc('start_scraper')
    def post(self):
        """Start the web scraper script"""
        try:
            data = {}
            if request.is_json:
                data = request.get_json() or {}
            elif request.form:
                data = request.form.to_dict()
            
            # Generate task ID
            task_id = generate_task_id()
            
            # Initialize task
            running_tasks[task_id] = {
                'task_id': task_id,
                'script': 'scraper',
                'status': 'initializing',
                'created_time': datetime.now(),
                'start_time': None,
                'end_time': None,
                'logs': [],
                'progress': 'Initializing scraper...',
                'error': None,
                'return_code': None
            }
            
            # Prepare script arguments
            args = []
            if 'batch_size' in data:
                args.extend(['--batch-size', str(data['batch_size'])])
            if 'days_back' in data:
                args.extend(['--days-back', str(data['days_back'])])
            if 'concurrent_limit' in data:
                args.extend(['--concurrent-limit', str(data['concurrent_limit'])])
            if 'chunk_size' in data:
                args.extend(['--chunk-size', str(data['chunk_size'])])
            if 'retry_limit' in data:
                args.extend(['--retry-limit', str(data['retry_limit'])])
            if 'retry_delay' in data:
                args.extend(['--retry-delay', str(data['retry_delay'])])
            
            # Verify script exists before starting thread
            try:
                script_path = find_script('Scrape_Adzuna_Table_Azure.py')
                logger.info(f"Script found at: {script_path}")
            except FileNotFoundError as e:
                running_tasks[task_id]['status'] = 'failed'
                running_tasks[task_id]['error'] = str(e)
                return running_tasks[task_id], 404
            
            # Start the script in a separate thread
            thread = threading.Thread(
                target=run_script_async,
                args=(task_id, 'Scrape_Adzuna_Table_Azure.py', args)
            )
            thread.daemon = True
            thread.start()
            
            return running_tasks[task_id], 202
            
        except Exception as e:
            logger.error(f"Error starting scraper: {str(e)}")
            return {'error': f'Failed to start scraper: {str(e)}'}, 500

@ai_ns.route('/start')
class StartAIProcessing(Resource):
    @ai_ns.expect(ai_config_model, validate=False)
    @ai_ns.marshal_with(task_status_model)
    @ai_ns.doc('start_ai_processing')
    def post(self):
        """Start the Gemini AI processing script"""
        try:
            data = {}
            if request.is_json:
                data = request.get_json() or {}
            elif request.form:
                data = request.form.to_dict()
            
            # Generate task ID
            task_id = generate_task_id()
            
            # Initialize task
            running_tasks[task_id] = {
                'task_id': task_id,
                'script': 'ai_processing',
                'status': 'initializing',
                'created_time': datetime.now(),
                'start_time': None,
                'end_time': None,
                'logs': [],
                'progress': 'Initializing AI processing...',
                'error': None,
                'return_code': None
            }
            
            # Prepare script arguments
            args = []
            if 'batch_size' in data:
                args.extend(['--batch-size', str(data['batch_size'])])
            if 'days_back' in data:
                args.extend(['--days-back', str(data['days_back'])])
            
            # Start the script in a separate thread
            thread = threading.Thread(
                target=run_script_async,
                args=(task_id, 'Gemini_AI_Azure.py', args)
            )
            thread.daemon = True
            thread.start()
            
            return running_tasks[task_id], 202
            
        except Exception as e:
            logger.error(f"Error starting AI processing: {str(e)}")
            return {'error': f'Failed to start AI processing: {str(e)}'}, 500

@company_ns.route('/start')
class StartCompanyURLProcessing(Resource):
    @company_ns.expect(company_url_config_model, validate=False)
    @company_ns.marshal_with(task_status_model)
    @company_ns.doc('start_company_url_processing')
    def post(self):
        """Start the Gemini AI Company URL processing script"""
        try:
            data = {}
            if request.is_json:
                data = request.get_json() or {}
            elif request.form:
                data = request.form.to_dict()
            
            # Generate task ID
            task_id = generate_task_id()
            
            # Initialize task
            running_tasks[task_id] = {
                'task_id': task_id,
                'script': 'company_url_processing',
                'status': 'initializing',
                'created_time': datetime.now(),
                'start_time': None,
                'end_time': None,
                'logs': [],
                'progress': 'Initializing company URL processing...',
                'error': None,
                'return_code': None
            }
            
            # Start the script in a separate thread
            thread = threading.Thread(
                target=run_script_async,
                args=(task_id, 'Gemini_AI_CompanyURL_Azure.py', [])
            )
            thread.daemon = True
            thread.start()
            
            return running_tasks[task_id], 202
            
        except Exception as e:
            logger.error(f"Error starting company URL processing: {str(e)}")
            return {'error': f'Failed to start company URL processing: {str(e)}'}, 500

@scraper_ns.route('/test')
class TestScraper(Resource):
    @scraper_ns.doc('test_scraper')
    def get(self):
        """Test if the scraper script can be found and executed"""
        try:
            script_path = find_script('Scrape_Adzuna_Table_Azure.py')
            
            # Test basic execution (dry run with help)
            result = subprocess.run(
                [sys.executable, str(script_path), '--help'],
                cwd=str(script_path.parent),
                capture_output=True,
                text=True,
                timeout=30
            )
            
            return {
                'script_found': True,
                'script_path': str(script_path),
                'help_output': result.stdout,
                'return_code': result.returncode,
                'error': result.stderr if result.stderr else None
            }
            
        except FileNotFoundError as e:
            return {'script_found': False, 'error': str(e)}, 404
        except subprocess.TimeoutExpired:
            return {'script_found': True, 'error': 'Script help command timed out'}, 500
        except Exception as e:
            return {'script_found': False, 'error': f'Unexpected error: {str(e)}'}, 500

@coordinates_ns.route('/start')
class StartCoordinatesProcessing(Resource):
    @coordinates_ns.expect(coordinates_config_model, validate=False)
    @coordinates_ns.marshal_with(task_status_model)
    @coordinates_ns.doc('start_coordinates_processing')
    def post(self):
        """Start the Google Maps coordinates processing script"""
        try:
            data = {}
            if request.is_json:
                data = request.get_json() or {}
            elif request.form:
                data = request.form.to_dict()

            # Generate task ID
            task_id = generate_task_id()

            # Initialize task
            running_tasks[task_id] = {
                'task_id': task_id,
                'script': 'coordinates_processing',
                'status': 'initializing',
                'created_time': datetime.now(),
                'start_time': None,
                'end_time': None,
                'logs': [],
                'progress': 'Initializing coordinates processing...',
                'error': None,
                'return_code': None
            }

            # Prepare script arguments
            args = []
            if 'batch_size' in data:
                args.extend(['--batch-size', str(data['batch_size'])])
            if 'delay' in data:
                args.extend(['--delay', str(data['delay'])])

            # Verify script exists before starting thread
            try:
                script_path = find_script('GoogleMapsAPI_Coordinates_Azure.py')
                logger.info(f"Script found at: {script_path}")
            except FileNotFoundError as e:
                running_tasks[task_id]['status'] = 'failed'
                running_tasks[task_id]['error'] = str(e)
                return running_tasks[task_id], 404

            # Start the script in a separate thread
            thread = threading.Thread(
                target=run_script_async,
                args=(task_id, 'GoogleMapsAPI_Coordinates_Azure.py', args)
            )
            thread.daemon = True
            thread.start()

            return running_tasks[task_id], 202

        except Exception as e:
            logger.error(f"Error starting coordinates processing: {str(e)}")
            return {'error': f'Failed to start coordinates processing: {str(e)}'}, 500

@coordinates_ns.route('/test')
class TestCoordinates(Resource):
    @coordinates_ns.doc('test_coordinates')
    def get(self):
        """Test if the coordinates script can be found and executed"""
        try:
            script_path = find_script('GoogleMapsAPI_Coordinates_Azure.py')
            
            result = subprocess.run(
                [sys.executable, str(script_path), '--help'],
                cwd=str(script_path.parent),
                capture_output=True,
                text=True,
                timeout=30
            )
            
            return {
                'script_found': True,
                'script_path': str(script_path),
                'help_output': result.stdout,
                'return_code': result.returncode,
                'error': result.stderr if result.stderr else None
            }
            
        except FileNotFoundError as e:
            return {'script_found': False, 'error': str(e)}, 404
        except subprocess.TimeoutExpired:
            return {'script_found': True, 'error': 'Script help command timed out'}, 500
        except Exception as e:
            return {'script_found': False, 'error': f'Unexpected error: {str(e)}'}, 500

@status_ns.route('/tasks')
class TaskList(Resource):
    @status_ns.doc('list_tasks')
    def get(self):
        """Get all tasks and their statuses"""
        try:
            # Create a safe copy of tasks for serialization
            safe_tasks = []
            for task_id, task in running_tasks.items():
                safe_task = task.copy()
                # Remove non-serializable process object if it exists
                if 'process' in safe_task:
                    del safe_task['process']
                # Ensure datetime objects are properly handled
                for field in ['created_time', 'start_time', 'end_time']:
                    if safe_task.get(field) and isinstance(safe_task[field], datetime):
                        safe_task[field] = safe_task[field].isoformat()
                safe_tasks.append(safe_task)
            
            return {
                'tasks': safe_tasks,
                'total_tasks': len(running_tasks),
                'running_tasks': len([t for t in running_tasks.values() if t['status'] == 'running'])
            }
            
        except Exception as e:
            logger.error(f"Error getting task list: {str(e)}")
            return {'error': f'Failed to get task list: {str(e)}'}, 500

@status_ns.route('/tasks/<string:task_id>')
class TaskDetails(Resource):
    @status_ns.marshal_with(task_status_model)
    @status_ns.doc('get_task_details')
    def get(self, task_id):
        """Get details for a specific task"""
        try:
            if task_id not in running_tasks:
                api.abort(404, f"Task {task_id} not found")
            
            task = running_tasks[task_id].copy()
            # Remove non-serializable process object if it exists
            if 'process' in task:
                del task['process']
            return task
            
        except Exception as e:
            logger.error(f"Error getting task details for {task_id}: {str(e)}")
            return {'error': f'Failed to get task details: {str(e)}'}, 500
    
    @status_ns.doc('cancel_task')
    def delete(self, task_id):
        """Cancel a running task"""
        try:
            if task_id not in running_tasks:
                api.abort(404, f"Task {task_id} not found")
            
            task = running_tasks[task_id]
            if task['status'] != 'running':
                return {'message': f"Task {task_id} is not running (status: {task['status']})"}, 400
            
            # Try to terminate the process
            if 'process' in task:
                try:
                    process = task['process']
                    # Try graceful termination first
                    if hasattr(os, 'killpg'):
                        # Kill entire process group if possible (Unix)
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    else:
                        # Windows or fallback
                        process.terminate()
                    
                    # Wait a bit for graceful shutdown
                    time.sleep(2)
                    
                    # Force kill if still running
                    if process.poll() is None:
                        if hasattr(os, 'killpg'):
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                        else:
                            process.kill()
                            
                except Exception as e:
                    logger.warning(f"Error terminating task {task_id}: {e}")
            
            # Update task status
            task['status'] = 'cancelled'
            task['end_time'] = datetime.now()
            task['progress'] = 'Task cancelled by user'
            
            return {'message': f"Task {task_id} cancelled successfully"}
            
        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {str(e)}")
            return {'error': f'Failed to cancel task: {str(e)}'}, 500

@status_ns.route('/system')
class SystemStatus(Resource):
    @status_ns.doc('system_status')
    def get(self):
        """Get system status and health information"""
        try:
            try:
                import psutil
                system_info = {
                    'platform': sys.platform,
                    'python_version': sys.version,
                    'cpu_count': psutil.cpu_count(),
                    'memory_total': psutil.virtual_memory().total,
                    'memory_available': psutil.virtual_memory().available,
                    'disk_usage': {
                        'total': psutil.disk_usage('/').total if os.name != 'nt' else psutil.disk_usage('C:').total,
                        'free': psutil.disk_usage('/').free if os.name != 'nt' else psutil.disk_usage('C:').free
                    }
                }
            except ImportError:
                system_info = {
                    'platform': sys.platform,
                    'python_version': sys.version,
                    'note': 'Install psutil for detailed system info'
                }
            except Exception as e:
                system_info = {
                    'platform': sys.platform,
                    'python_version': sys.version,
                    'error': f'Could not get detailed system info: {str(e)}'
                }
            
            return {
                'system_info': system_info,
                'application_status': {
                    'uptime': time.time() - start_time,
                    'total_tasks': len(running_tasks),
                    'running_tasks': len([t for t in running_tasks.values() if t['status'] == 'running']),
                    'completed_tasks': len([t for t in running_tasks.values() if t['status'] == 'completed']),
                    'failed_tasks': len([t for t in running_tasks.values() if t['status'] == 'failed'])
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {str(e)}")
            return {'error': f'Failed to get system status: {str(e)}'}, 500

@app.route('/')
def index():
    """Redirect to Swagger UI"""
    return '''
    <h1>Adzuna Job Processing API</h1>
    <p>Welcome to the Adzuna Job Processing API!</p>
    <p><a href="/docs/">Access the API documentation (Swagger UI)</a></p>
    <p><strong>Available endpoints:</strong></p>
    <ul>
    <li><strong>POST /api/v1/scraper/start</strong> - Start web scraping</li>
    <li><strong>GET /api/v1/scraper/test</strong> - Test scraper script availability</li>
    <li><strong>POST /api/v1/ai/start</strong> - Start AI processing</li>
    <li><strong>POST /api/v1/company/start</strong> - Start Company URL processing</li>
    <li><strong>POST /api/v1/coordinates/start</strong> - Start coordinates processing</li>
    <li><strong>GET /api/v1/coordinates/test</strong> - Test coordinates script availability</li>
    <li><strong>GET /api/v1/status/tasks</strong> - List all tasks</li>
    <li><strong>GET /api/v1/status/tasks/{task_id}</strong> - Get task details</li>
    <li><strong>DELETE /api/v1/status/tasks/{task_id}</strong> - Cancel task</li>
    <li><strong>GET /api/v1/status/system</strong> - System status</li>
    </ul>
    '''

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'running_tasks': len([t for t in running_tasks.values() if t['status'] == 'running'])
    })

# Track application start time
start_time = time.time()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    logger.info(f"Starting Flask application on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)