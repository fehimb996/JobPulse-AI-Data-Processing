import os
import pyodbc
from google import genai
from google.genai import types
import json
import textwrap
import time
from typing import List, Tuple, Optional, Dict
from datetime import datetime, timedelta
from contextlib import contextmanager
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
                    time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
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

def get_existing_languages_and_skills() -> Tuple[Dict[str, int], Dict[str, int]]:
    """Get existing languages and skills from database to avoid duplicates."""
    languages = {}
    skills = {}
    
    try:
        # Get languages
        lang_results = db_manager.execute_with_retry(
            "SELECT Id, Name FROM Languages",
            fetch_method='fetchall'
        )
        for row in lang_results:
            languages[row.Name.lower()] = row.Id
        
        # Get skills
        skill_results = db_manager.execute_with_retry(
            "SELECT Id, SkillName FROM Skills",
            fetch_method='fetchall'
        )
        for row in skill_results:
            skills[row.SkillName.lower()] = row.Id
            
    except Exception as e:
        print(f"-> Error fetching existing languages/skills: {e}")
    
    return languages, skills

def insert_new_language(language_name: str) -> Optional[int]:
    """Insert a new language and return its ID."""
    try:
        return db_manager.execute_with_retry(
            "INSERT INTO Languages (Name) VALUES (?)",
            (language_name,),
            fetch_method='identity'
        )
    except Exception as e:
        print(f"-> Error inserting language '{language_name}': {e}")
        return None

def insert_new_skill(skill_name: str) -> Optional[int]:
    """Insert a new skill and return its ID."""
    try:
        return db_manager.execute_with_retry(
            "INSERT INTO Skills (SkillName) VALUES (?)",
            (skill_name,),
            fetch_method='identity'
        )
    except Exception as e:
        print(f"-> Error inserting skill '{skill_name}': {e}")
        return None

def link_job_languages_merge(job_post_id: int, language_ids: List[int]):
    """Link job post to languages using MERGE statement with improved connection handling."""
    if not language_ids:
        return
        
    successful_inserts = 0
    
    for lang_id in language_ids:
        try:
            db_manager.execute_with_retry("""
                MERGE JobPostLanguages AS target
                USING (SELECT ? AS JobPostId, ? AS LanguageId) AS source
                ON target.JobPostId = source.JobPostId AND target.LanguageId = source.LanguageId
                WHEN NOT MATCHED THEN
                    INSERT (JobPostId, LanguageId) VALUES (source.JobPostId, source.LanguageId);
            """, (job_post_id, lang_id))
            successful_inserts += 1
        except Exception as e:
            print(f"-> Error in MERGE for job {job_post_id}, language {lang_id}: {e}")
    
    if successful_inserts > 0:
        print(f"-> Successfully processed {successful_inserts}/{len(language_ids)} languages for job {job_post_id}")

def link_job_skills_merge(job_post_id: int, skill_ids: List[int]):
    """Link job post to skills using MERGE statement with improved connection handling."""
    if not skill_ids:
        return
        
    successful_inserts = 0
    
    for skill_id in skill_ids:
        try:
            db_manager.execute_with_retry("""
                MERGE JobPostSkills AS target
                USING (SELECT ? AS JobPostId, ? AS SkillId) AS source
                ON target.JobPostId = source.JobPostId AND target.SkillId = source.SkillId
                WHEN NOT MATCHED THEN
                    INSERT (JobPostId, SkillId) VALUES (source.JobPostId, source.SkillId);
            """, (job_post_id, skill_id))
            successful_inserts += 1
        except Exception as e:
            print(f"-> Error in MERGE for job {job_post_id}, skill {skill_id}: {e}")
    
    if successful_inserts > 0:
        print(f"-> Successfully processed {successful_inserts}/{len(skill_ids)} skills for job {job_post_id}")

def update_workplace_model(job_post_id: int, work_location: Optional[str]):
    """Update the WorkplaceModelId for the job post."""
    try:
        if work_location is None:
            # Default to OnSite (ID = 1) if not specified
            db_manager.execute_with_retry(
                "UPDATE JobPosts SET WorkplaceModelId = 1 WHERE Id = ?",
                (job_post_id,)
            )
        else:
            workplace_mapping = {"OnSite": 1, "Remote": 2, "Hybrid": 3}
            workplace_id = workplace_mapping.get(work_location)
            if workplace_id:
                db_manager.execute_with_retry(
                    "UPDATE JobPosts SET WorkplaceModelId = ? WHERE Id = ?",
                    (workplace_id, job_post_id)
                )
    except Exception as e:
        print(f"-> Error updating workplace for job {job_post_id}: {e}")

def mark_job_inactive(job_post_id: int):
    """Mark job as inactive by setting IsInactive = 1."""
    try:
        db_manager.execute_with_retry(
            "UPDATE JobPosts SET IsInactive = 1 WHERE Id = ?",
            (job_post_id,)
        )
        print(f"-> Marked job {job_post_id} as inactive")
    except Exception as e:
        print(f"-> Error marking job {job_post_id} as inactive: {e}")

def normalize_language_names(languages: List[str]) -> List[str]:
    """Normalize language names to English equivalents."""
    language_mapping = {
        "deutsch": "German", "deutsche": "German", "german": "German",
        "englisch": "English", "english": "English",
        "français": "French", "francais": "French", "french": "French",
        "español": "Spanish", "espanol": "Spanish", "spanish": "Spanish",
        "nederlands": "Dutch", "dutch": "Dutch",
        "italiano": "Italian", "italian": "Italian",
        "português": "Portuguese", "portugues": "Portuguese", "portuguese": "Portuguese",
        "中文": "Chinese", "chinese": "Chinese", "mandarin": "Chinese",
        "日本語": "Japanese", "japanese": "Japanese",
        "العربية": "Arabic", "arabic": "Arabic",
        "русский": "Russian", "russian": "Russian",
    }
    
    normalized = []
    for lang in languages:
        lang_lower = lang.strip().lower()
        normalized_lang = language_mapping.get(lang_lower, lang.strip())
        if normalized_lang and normalized_lang not in normalized:
            normalized.append(normalized_lang)
    
    return normalized

def normalize_skill_names(skills: List[str]) -> List[str]:
    """Normalize skill names to standard English equivalents."""
    skill_mapping = {
        "javascript": "JavaScript", "typescript": "TypeScript", 
        "c#": "C#", "c++": "C++", "node.js": "Node.js",
        "react.js": "React", "vue.js": "Vue.js", "angular.js": "Angular",
        "postgresql": "PostgreSQL", "mysql": "MySQL", "mongodb": "MongoDB", "sqlite": "SQLite",
        "amazon web services": "AWS", "google cloud platform": "GCP", "microsoft azure": "Azure",
        "datenbank": "Database", "programmierung": "Programming", 
        "entwicklung": "Development", "software entwicklung": "Software Development",
    }
    
    normalized = []
    for skill in skills:
        skill_lower = skill.strip().lower()
        normalized_skill = skill_mapping.get(skill_lower, skill.strip())
        if normalized_skill and normalized_skill not in normalized:
            normalized.append(normalized_skill)
    
    return normalized

def is_job_inactive(full_description: str) -> bool:
    """Check if job is inactive based on description content."""
    if not full_description:
        return False
    
    inactive_phrases = [
        "this job is no longer available",
        "job is no longer available", 
        "position is no longer available",
        "role is no longer available",
        "posting has expired",
        "application closed"
    ]
    
    desc_lower = full_description.lower().strip()
    return any(phrase in desc_lower for phrase in inactive_phrases)

def process_job_description(job_data: Tuple[int, str]) -> Optional[Tuple[int, List[str], List[str], Optional[str], bool]]:
    """Process a single job description and return parsed results."""
    job_post_id, full_description = job_data
    
    if not full_description or not isinstance(full_description, str):
        print(f"-> Invalid job description for job {job_post_id}")
        return None
    
    # Check if job is inactive first
    is_inactive = is_job_inactive(full_description)
    
    if is_inactive:
        print(f"-> Job {job_post_id} is inactive - skipping AI processing")
        return (job_post_id, [], [], None, True)
    
    # Truncate if too long
    if len(full_description) > 20000:
        full_description = full_description[:20000]
    
    prompt = textwrap.dedent(f"""\
        CRITICAL: You MUST return ONLY valid JSON in ENGLISH language. No explanations, no additional text.
        
        Analyze this job posting and return EXACTLY this JSON format:
        {{"skills": ["skill1", "skill2", "skill3"], "languages": ["language1", "language2"], "work_location": "Remote, OnSite or Hybrid"}}
        
        MANDATORY RULES:
        - ALL responses must be in ENGLISH only
        - skills: Array of ALL technical skills, programming languages, frameworks, tools, platforms, methodologies, etc. 
          Examples: ["Python", "JavaScript", "React", "Node.js", "SQL", "PostgreSQL", "AWS", "Azure", "Docker", "Kubernetes", "Git", "Agile", "Scrum", "Machine Learning", "Data Engineering", ".NET", "C#", "Java", "Spring Boot", "Microservices", "REST API", "GraphQL", "MongoDB", "Redis", "Jenkins", "CI/CD", "Terraform", "Linux", "Bash", "DevOps", "Cybersecurity"]
        - languages: Array of SPOKEN LANGUAGES in ENGLISH names only
          CORRECT: ["English", "German", "French", "Spanish", "Dutch", "Italian", "Portuguese", "Chinese", "Japanese", "Arabic"]
          WRONG: ["Englisch", "Deutsch", "Français", "Español", "Nederlands", "Italiano", "Português", "中文", "日本語", "العربية"]
        - work_location: ONLY "Remote", "Hybrid", or "OnSite" (in English)
        - If unclear/incomplete job: {{"skills": [], "languages": [], "work_location": "NULL"}}
        - TRANSLATE any non-English skill or language names to their English equivalents
        - Use standardized English names for all programming languages and technologies
        - Return ONLY the JSON object, nothing else
        
        LANGUAGE TRANSLATION EXAMPLES:
        - "Deutsch" → "German"
        - "Français" → "French" 
        - "Español" → "Spanish"
        - "Nederlands" → "Dutch"
        - "Italiano" → "Italian"
        - "Português" → "Portuguese"
        - "Englisch" → "English"
        
        Job posting:
        \"\"\"{full_description}\"\"\"
    """)
    
    try:
        response_text = send_to_gemini(prompt)
        
        if not response_text:
            print(f"-> No response from Gemini API for job {job_post_id}")
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
            print(f"-> Empty response after cleaning for job {job_post_id}")
            return None
        
        print(f"-> Processed job {job_post_id}")
        
        # Parse JSON
        try:
            data = json.loads(generated_text)
        except json.JSONDecodeError as e:
            print(f"-> JSON decode error for job {job_post_id}: {e}")
            print(f"   Raw response: {response_text[:200]}...")
            print(f"   Cleaned text: {generated_text[:100]}...")
            return None
        
        # Handle list response
        if isinstance(data, list):
            if len(data) > 0 and isinstance(data[0], dict):
                print(f"->  Got list response for job {job_post_id}, using first item")
                data = data[0]
            else:
                print(f"-> Invalid list format for job {job_post_id}")
                return None
        elif not isinstance(data, dict):
            print(f"-> Expected dict or list but got {type(data)} for job {job_post_id}")
            return None
        
        # Extract data
        skills = data.get("skills", []) if isinstance(data.get("skills"), list) else []
        languages = data.get("languages", []) if isinstance(data.get("languages"), list) else []
        work_location = data.get("work_location", "").strip() if data.get("work_location") else ""
        
        # Normalize to English
        languages = normalize_language_names(languages)
        skills = normalize_skill_names(skills)
        
        # Validate work location - if not specified or invalid, it will default to OnSite in update_workplace_model
        valid_locations = ["Remote", "Hybrid", "OnSite"]
        if work_location not in valid_locations:
            work_location = None  # Will default to OnSite
        
        return (job_post_id, skills, languages, work_location, False)
        
    except Exception as e:
        print(f"-> Unexpected error processing job {job_post_id}: {e}")
        return None

def process_and_link_job_data(job_result: Tuple[int, List[str], List[str], Optional[str], bool], 
                            existing_languages: Dict[str, int], existing_skills: Dict[str, int]) -> bool:
    """Process job result and link to database tables with improved error handling."""
    job_post_id, skills, languages, work_location, is_inactive = job_result
    
    try:
        # If job is inactive, mark it and skip processing
        if is_inactive:
            mark_job_inactive(job_post_id)
            return True
        
        # Check if job already processed
        check_results = db_manager.execute_with_retry("""
            SELECT COUNT(*) FROM JobPostSkills WHERE JobPostId = ?
            UNION ALL
            SELECT COUNT(*) FROM JobPostLanguages WHERE JobPostId = ?
        """, (job_post_id, job_post_id), fetch_method='fetchall')
        
        if any(count[0] > 0 for count in check_results):
            print(f"->  Job {job_post_id} already has skills/languages linked, skipping...")
            return True
        
        # Process languages
        language_ids = []
        for lang in languages:
            lang_lower = lang.strip().lower()
            if lang_lower in existing_languages:
                language_ids.append(existing_languages[lang_lower])
            else:
                new_lang_id = insert_new_language(lang.strip())
                if new_lang_id:
                    existing_languages[lang_lower] = new_lang_id
                    language_ids.append(new_lang_id)
        
        # Process skills
        skill_ids = []
        for skill in skills:
            skill_lower = skill.strip().lower()
            if skill_lower in existing_skills:
                skill_ids.append(existing_skills[skill_lower])
            else:
                new_skill_id = insert_new_skill(skill.strip())
                if new_skill_id:
                    existing_skills[skill_lower] = new_skill_id
                    skill_ids.append(new_skill_id)
        
        # Link to database using MERGE statements
        if language_ids:
            link_job_languages_merge(job_post_id, language_ids)
        
        if skill_ids:
            link_job_skills_merge(job_post_id, skill_ids)
        
        # Update workplace model (defaults to OnSite if work_location is None)
        update_workplace_model(job_post_id, work_location)
        
        return True
        
    except Exception as e:
        print(f"-> Error processing job data for {job_post_id}: {e}")
        return False

def fetch_jobs_batch(batch_size: int = 100, days_back: int = 30) -> List[Tuple[int, str]]:
    """Fetch a batch of jobs from the database, filtered by creation date."""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        return db_manager.execute_with_retry(f"""
            SELECT TOP {batch_size} jp.Id, jp.FullDescription
            FROM JobPosts jp
            WHERE jp.FullDescription IS NOT NULL 
                AND jp.Created >= ?
                AND NOT EXISTS (SELECT 1 FROM JobPostLanguages WHERE JobPostId = jp.Id)
                AND NOT EXISTS (SELECT 1 FROM JobPostSkills WHERE JobPostId = jp.Id)
            ORDER BY jp.Created DESC, jp.Id
        """, (cutoff_date,), fetch_method='fetchall')
        
    except Exception as e:
        print(f"-> Database fetch error: {e}")
        return []

def process_jobs_efficiently(batch_size: int = 100, days_back: int = 30):
    """Process jobs efficiently with enhanced connection management and resumability."""
    try:
        # Get existing languages and skills
        existing_languages, existing_skills = get_existing_languages_and_skills()
        print(f"-> Found {len(existing_languages)} existing languages and {len(existing_skills)} existing skills")
        
        # Count total jobs to process
        cutoff_date = datetime.now() - timedelta(days=days_back)
        total_jobs_result = db_manager.execute_with_retry("""
            SELECT COUNT(*) FROM JobPosts 
            WHERE FullDescription IS NOT NULL AND Created >= ?
                AND NOT EXISTS (SELECT 1 FROM JobPostLanguages WHERE JobPostId = JobPosts.Id)
                AND NOT EXISTS (SELECT 1 FROM JobPostSkills WHERE JobPostId = JobPosts.Id)
        """, (cutoff_date,), fetch_method='fetchone')
        
        total_jobs = total_jobs_result[0]
        print(f"-> Total jobs to process (last {days_back} days): {total_jobs}")
        print(f"-> Processing jobs created after: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        processed_count = 0
        failed_count = 0
        inactive_count = 0
        
        # Process in batches with regular connection refresh
        while True:
            jobs_batch = fetch_jobs_batch(batch_size, days_back)
            
            if not jobs_batch:
                print("-> No more jobs to process")
                break
            
            print(f"-> Processing batch of {len(jobs_batch)} jobs...")
            successful_updates = 0
            
            for job_data in jobs_batch:
                result = process_job_description(job_data)
                if result:
                    if result[4]:  # is_inactive flag
                        inactive_count += 1
                    
                    if process_and_link_job_data(result, existing_languages, existing_skills):
                        successful_updates += 1
                        if processed_count < 3 and not result[4]:  # Show sample output for active jobs
                            print(f"   Sample - Job {result[0]}: Skills: {result[1][:3]}, Languages: {result[2][:3]}, Location: {result[3] or 'OnSite (default)'}")
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
                
                time.sleep(0.5)  # Rate limiting
            
            processed_count += successful_updates
            print(f"-> Updated {successful_updates} jobs in batch. Total processed: {processed_count}/{total_jobs}, Failed: {failed_count}, Inactive: {inactive_count}")
            
            # Refresh language/skill cache periodically
            if processed_count % 500 == 0:
                print("-> Refreshing language/skill cache...")
                existing_languages, existing_skills = get_existing_languages_and_skills()
            
            time.sleep(1.0)  # Brief pause between batches
    
    except Exception as e:
        print(f"-> Critical error: {e}")
    
    except KeyboardInterrupt:
        print(f"\n->  Process interrupted. Total processed: {processed_count}, Failed: {failed_count}, Inactive: {inactive_count}")
    
    finally:
        print(f"-> Processing complete. Total jobs processed: {processed_count}, Failed: {failed_count}, Inactive jobs marked: {inactive_count}")

if __name__ == "__main__":
    process_jobs_efficiently(batch_size=50, days_back=14)