# llm_scraper.py (UPDATED - with actual timestamp calculation)
import os
import requests
from dotenv import load_dotenv
import json
import logging
import re
from typing import Dict, Optional, List
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata")

# List of free models to try in order
FREE_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
    "meta-llama/llama-4-maverick:free",
]

def extract_linkedin_job_id_from_url(job_url: str) -> str:
    """
    Generate reliable job_id from cleaned LinkedIn URL.
    
    Removes tracking parameters (refId, trackingId) but keeps the core URL.
    This ensures the same job always has the same ID regardless of tracking params.
    
    Args:
        job_url: Full LinkedIn job URL with tracking parameters
        
    Returns:
        Hash of cleaned URL as job_id
    """
    import hashlib
    from urllib.parse import urlparse, parse_qs, urlencode
    
    try:
        # Parse URL
        parsed = urlparse(job_url)
        
        # Get the path (e.g., /jobs/view/title-at-company-4314028712)
        path = parsed.path
        
        # Parse query parameters
        query_params = parse_qs(parsed.query)
        
        # Remove tracking parameters but keep important ones
        tracking_params = ['refId', 'trackingId', 'trk', 'trkInfo', 'trkEmail']
        
        # Keep only non-tracking parameters (like position, pageNum if you want)
        # Actually, let's remove ALL query params for consistency
        clean_params = {}
        
        # Reconstruct clean URL (just scheme + netloc + path)
        clean_url = f"{parsed.scheme}://{parsed.netloc}{path}"
        
        # Generate consistent hash from clean URL
        job_id = hashlib.md5(clean_url.encode()).hexdigest()
        
        return job_id
        
    except Exception as e:
        logging.error(f"Error extracting job ID from URL: {e}")
        # Ultimate fallback
        import hashlib
        return hashlib.md5(job_url.encode()).hexdigest()


def get_clean_linkedin_url(job_url: str) -> str:
    """
    Get clean LinkedIn URL without tracking parameters.
    
    Args:
        job_url: Full URL with tracking
        
    Returns:
        Clean URL without tracking parameters
    """
    try:
        from urllib.parse import urlparse
        parsed = urlparse(job_url)
        # Return just scheme + netloc + path (no query params)
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return clean_url
    except Exception as e:
        logging.error(f"Error cleaning URL: {e}")
        # Fallback: split on ?
        return job_url.split('?')[0]



def calculate_posted_at_timestamp(time_posted_hours: Optional[int]) -> Optional[str]:
    """
    Calculate actual timestamp when job was posted.
    
    Args:
        time_posted_hours: How many hours ago the job was posted
    
    Returns:
        ISO format timestamp of when job was actually posted, or None
    """
    if time_posted_hours is None:
        return None
    
    try:
        current_time = datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
        posted_at = current_time - timedelta(hours=time_posted_hours)
        return posted_at.isoformat()
    except Exception as e:
        logging.error(f"Error calculating posted_at timestamp: {e}")
        return None


def clean_html_for_llm(html_content: str) -> str:
    """
    Clean HTML content for LLM processing.
    Removes HTML tags, CSS, scripts, and excessive whitespace.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script in soup(["script", "style", "meta", "link"]):
        script.decompose()
    
    # Get text content
    text = soup.get_text(separator=' ', strip=True)
    
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s\.,\-:;()\[\]\/]', '', text)
    
    return text.strip()


def clean_job_description(description: str) -> str:
    """Clean job description text for LLM."""
    # Remove HTML if present
    if '<' in description and '>' in description:
        soup = BeautifulSoup(description, 'html.parser')
        description = soup.get_text(separator='\n', strip=True)
    
    # Remove excessive whitespace
    description = re.sub(r'\n\s*\n', '\n\n', description)
    description = re.sub(r' +', ' ', description)
    
    return description.strip()


def extract_json_from_text(text: str) -> Optional[Dict]:
    """
    Extract JSON from LLM response that might have extra text.
    Tries multiple strategies to find and parse JSON.
    """
    # Strategy 1: Try direct JSON parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # Strategy 2: Find JSON object between curly braces
    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    matches = re.finditer(json_pattern, text, re.DOTALL)
    
    for match in matches:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            continue
    
    # Strategy 3: Look for code blocks
    code_block_pattern = r'``````'
    match = re.search(code_block_pattern, text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    
    return None


class LLMJobScraper:
    """LLM-enhanced job metadata extraction with multiple model fallback."""
    
    def __init__(self, models: List[str] = None):
        """Initialize with list of models to try."""
        self.models = models or FREE_MODELS
        self.headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:8000",
            "X-Title": "Job Scraper AI"
        }
    
    def _call_llm(self, messages: list, retry_models: bool = True) -> Dict:
        """
        Make API call to OpenRouter with multiple model fallback.
        
        Tries each model in self.models list until one succeeds.
        """
        last_error = None
        
        models_to_try = self.models if retry_models else [self.models[0]]
        
        for model in models_to_try:
            try:
                payload = {
                    "model": model,
                    "messages": messages,
                    "temperature": 0.1,
                }
                
                response = requests.post(
                    OPENROUTER_BASE_URL,
                    headers=self.headers,
                    data=json.dumps(payload),
                    timeout=30
                )
                response.raise_for_status()
                
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                # Try to extract JSON from response
                parsed_json = extract_json_from_text(content)
                
                if parsed_json:
                    parsed_json['_llm_model_used'] = model
                    return parsed_json
                else:
                    logging.warning(f"Model {model} returned non-JSON response")
                    last_error = {"error": "invalid_json", "raw_response": content}
                    continue
                
            except requests.exceptions.RequestException as e:
                logging.warning(f"Model {model} failed: {e}")
                last_error = {"error": str(e)}
                continue
            except Exception as e:
                logging.warning(f"Unexpected error with model {model}: {e}")
                last_error = {"error": str(e)}
                continue
        
        # All models failed
        logging.error(f"All LLM models failed for scraping. Last error: {last_error}")
        return last_error or {"error": "all_models_failed"}
    
    def llm_extract_job_metadata(self, html_content: str, job_url: str) -> Optional[Dict]:
        """
        Use LLM to extract job metadata from HTML content.
        Tries multiple models before giving up.
        """
        if not OPENROUTER_API_KEY:
            logging.warning("OPENROUTER_API_KEY not set. Skipping LLM extraction.")
            return None
        
        # Clean HTML for LLM
        cleaned_content = clean_html_for_llm(html_content)
        
        # Limit to first 3000 characters
        cleaned_content = cleaned_content[:3000]
        
        prompt = f"""Analyze this LinkedIn job page content and extract metadata. Respond ONLY with valid JSON.

Page Content:
{cleaned_content}

Return ONLY this JSON structure (no markdown, no explanation):
{{
    "time_posted_text": "<exact text like '2 hours ago' or null>",
    "time_posted_hours": <hours ago as integer or null>,
    "applicant_count": <number as integer or null>,
    "applicant_count_text": "<exact text like '50 applicants' or null>",
    "job_title": "<job title or null>",
    "company_name": "<company name or null>",
    "location": "<location or null>",
    "employment_type": "<Full-time/Part-time/Contract/Internship or null>",
    "seniority_level": "<Entry level/Mid-Senior level/Director/Executive or null>",
    "workplace_type": "<On-site/Remote/Hybrid or null>"
}}

Important:
- time_posted_hours: Convert to hours (1 minute=0, 1 hour=1, 1 day=24, 1 week=168, 1 month=720)
- applicant_count: Extract number only (e.g., "Over 200" = 200, "first 25" = 25)
- Use exact text from page
- Use null if not found"""

        messages = [
            {
                "role": "system",
                "content": "You extract structured data from LinkedIn job pages. Respond ONLY with valid JSON, no other text."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        result = self._call_llm(messages, retry_models=True)
        
        if "error" in result:
            logging.error(f"LLM metadata extraction failed: {result['error']}")
            return None
        
        return result


def fallback_extract_metadata(soup: BeautifulSoup) -> Dict:
    """
    Fallback metadata extraction using BeautifulSoup and regex.
    Used when LLM extraction fails.
    """
    metadata = {
        "time_posted_text": None,
        "time_posted_hours": None,
        "applicant_count": None,
        "applicant_count_text": None,
        "employment_type": None,
        "seniority_level": None,
        "workplace_type": None
    }
    
    # Try to extract time posted
    try:
        time_elements = soup.select('.posted-time-ago__text, .topcard__flavor--metadata, time')
        for elem in time_elements:
            text = elem.get_text(strip=True)
            if 'ago' in text.lower():
                metadata['time_posted_text'] = text
                
                # Convert to hours
                if 'minute' in text.lower():
                    mins = re.search(r'(\d+)\s*minute', text)
                    if mins:
                        metadata['time_posted_hours'] = max(1, int(mins.group(1)) // 60)
                elif 'hour' in text.lower():
                    hours = re.search(r'(\d+)\s*hour', text)
                    if hours:
                        metadata['time_posted_hours'] = int(hours.group(1))
                elif 'day' in text.lower():
                    days = re.search(r'(\d+)\s*day', text)
                    if days:
                        metadata['time_posted_hours'] = int(days.group(1)) * 24
                elif 'week' in text.lower():
                    weeks = re.search(r'(\d+)\s*week', text)
                    if weeks:
                        metadata['time_posted_hours'] = int(weeks.group(1)) * 168
                elif 'month' in text.lower():
                    months = re.search(r'(\d+)\s*month', text)
                    if months:
                        metadata['time_posted_hours'] = int(months.group(1)) * 720
                break
    except Exception as e:
        logging.debug(f"Failed to extract time posted: {e}")
    
    # Try to extract applicant count
    try:
        applicant_elements = soup.select('.num-applicants__caption, .topcard__flavor--metadata')
        for elem in applicant_elements:
            text = elem.get_text(strip=True)
            if 'applicant' in text.lower():
                metadata['applicant_count_text'] = text
                
                # Extract number
                if 'over' in text.lower():
                    num = re.search(r'over\s*(\d+)', text, re.IGNORECASE)
                    if num:
                        metadata['applicant_count'] = int(num.group(1))
                elif 'first' in text.lower():
                    num = re.search(r'first\s*(\d+)', text, re.IGNORECASE)
                    if num:
                        metadata['applicant_count'] = int(num.group(1))
                else:
                    num = re.search(r'(\d+)', text)
                    if num:
                        metadata['applicant_count'] = int(num.group(1))
                break
    except Exception as e:
        logging.debug(f"Failed to extract applicant count: {e}")
    
    # Try to extract employment type and seniority
    try:
        job_criteria = soup.select('.description__job-criteria-item')
        for item in job_criteria:
            header = item.select_one('.description__job-criteria-subheader')
            if header:
                header_text = header.get_text(strip=True).lower()
                value = item.select_one('.description__job-criteria-text')
                
                if value:
                    value_text = value.get_text(strip=True)
                    
                    if 'employment type' in header_text:
                        metadata['employment_type'] = value_text
                    elif 'seniority level' in header_text:
                        metadata['seniority_level'] = value_text
                    elif 'job function' in header_text or 'workplace' in header_text:
                        if 'remote' in value_text.lower():
                            metadata['workplace_type'] = 'Remote'
                        elif 'hybrid' in value_text.lower():
                            metadata['workplace_type'] = 'Hybrid'
                        elif 'on-site' in value_text.lower() or 'onsite' in value_text.lower():
                            metadata['workplace_type'] = 'On-site'
    except Exception as e:
        logging.debug(f"Failed to extract job criteria: {e}")
    
    return metadata


def scrape_job_details_enhanced(browser, job_url):
    """
    Enhanced job scraping with LLM metadata extraction and actual timestamp calculation.
    """
    import hashlib
    
    page = browser.new_page()
    try:
        page.goto(job_url)
        import random
        import time
        time.sleep(random.uniform(0.5, 1.5))
        
        page.wait_for_selector(".show-more-less-html__markup", timeout=5000)
        html_content = page.content()
        
        # Generate reliable job_id from clean URL
        job_id = extract_linkedin_job_id_from_url(job_url)
        clean_url = get_clean_linkedin_url(job_url)
        
        print(f"  🆔 Job ID: {job_id}")
        print(f"  🔗 Clean URL: {clean_url}")
        html_filename = f"scraped_data/{job_id}.html"
        
        # Save HTML for debugging
        os.makedirs("scraped_data", exist_ok=True)
        with open(html_filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Extract basic details with BeautifulSoup (always reliable)
        job_title_element = soup.select_one(".top-card-layout__title")
        job_title = job_title_element.get_text(strip=True) if job_title_element else "N/A"
        
        company_name_element = soup.select_one(".top-card-layout__card a")
        company_name = company_name_element.get_text(strip=True) if company_name_element else "N/A"
        
        location_element = soup.select_one(".topcard__flavor--bullet")
        location = location_element.get_text(strip=True) if location_element else "N/A"
        
        description_element = soup.select_one(".show-more-less-html__markup")
        description = description_element.get_text("\n", strip=True) if description_element else "N/A"
        
        # Try LLM metadata extraction with multiple model fallback
        print(f"  🤖 Attempting LLM metadata extraction (trying {len(FREE_MODELS)} models)...")
        llm_scraper = LLMJobScraper(models=FREE_MODELS)
        llm_metadata = llm_scraper.llm_extract_job_metadata(html_content, job_url)
        
        if llm_metadata and "error" not in llm_metadata:
            llm_model_used = llm_metadata.pop('_llm_model_used', 'unknown')
            print(f"  ✅ LLM extraction successful with model: {llm_model_used}")
            print(f"    • Posted: {llm_metadata.get('time_posted_text', 'N/A')}")
            print(f"    • Applicants: {llm_metadata.get('applicant_count_text', 'N/A')}")
            print(f"    • Employment: {llm_metadata.get('employment_type', 'N/A')}")
            print(f"    • Workplace: {llm_metadata.get('workplace_type', 'N/A')}")
            
            metadata = llm_metadata
            metadata_source = "llm"
            metadata_model = llm_model_used
        else:
            print(f"  ⚠️  All LLM models failed, using BeautifulSoup fallback")
            metadata = fallback_extract_metadata(soup)
            metadata_source = "fallback"
            metadata_model = None
            print(f"    • Posted: {metadata.get('time_posted_text', 'N/A')}")
            print(f"    • Applicants: {metadata.get('applicant_count_text', 'N/A')}")
        
        # Calculate actual posted_at timestamp
        posted_at = calculate_posted_at_timestamp(metadata.get('time_posted_hours'))
        if posted_at:
            print(f"    • Calculated posted_at: {posted_at}")
        
        current_time = datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
        
        # Combine all data
        job_data = {
            "job_id": job_id,  # Hash of clean URL - reliable deduplication
            "url": clean_url,
            "company_name": company_name,
            "job_title": job_title,
            "location": location,
            "description": description,
            "html_file_path": html_filename,
            
            # Time metadata
            "time_posted_text": metadata.get('time_posted_text'),
            "time_posted_hours": metadata.get('time_posted_hours'),
            "posted_at": posted_at,  # NEW: Actual timestamp when job was posted
            
            # Applicant metadata
            "applicant_count": metadata.get('applicant_count'),
            "applicant_count_text": metadata.get('applicant_count_text'),
            
            # Job details
            "employment_type": metadata.get('employment_type'),
            "seniority_level": metadata.get('seniority_level'),
            "workplace_type": metadata.get('workplace_type'),
            
            # Tracking metadata
            "metadata_source": metadata_source,
            "metadata_model": metadata_model,
            "scraped_at": current_time.isoformat(),
        }
        
        return job_data
        
    except Exception as e:
        print(f"❌ Error scraping details for {job_url}: {e}")
        logging.error(f"Scraping error for {job_url}: {e}", exc_info=True)
        return None
    finally:
        page.close()


def scrape_job_details_basic(browser, job_url):
    """Basic scraper without LLM (ultimate fallback)."""
    import hashlib
    
    page = browser.new_page()
    try:
        page.goto(job_url)
        import random, time
        time.sleep(random.uniform(0.5, 1.5))
        
        page.wait_for_selector(".show-more-less-html__markup", timeout=5000)
        html_content = page.content()
        
        # Generate reliable job_id from clean URL
        job_id = extract_linkedin_job_id_from_url(job_url)
        clean_url = get_clean_linkedin_url(job_url)
        
        print(f"  🆔 Job ID: {job_id}")
        print(f"  🔗 Clean URL: {clean_url}")
        html_filename = f"scraped_data/{job_id}.html"
        
        os.makedirs("scraped_data", exist_ok=True)
        with open(html_filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        soup = BeautifulSoup(html_content, "html.parser")
        
        job_title_element = soup.select_one(".top-card-layout__title")
        job_title = job_title_element.get_text(strip=True) if job_title_element else "N/A"
        
        company_name_element = soup.select_one(".top-card-layout__card a")
        company_name = company_name_element.get_text(strip=True) if company_name_element else "N/A"
        
        location_element = soup.select_one(".topcard__flavor--bullet")
        location = location_element.get_text(strip=True) if location_element else "N/A"
        
        description_element = soup.select_one(".show-more-less-html__markup")
        description = description_element.get_text("\n", strip=True) if description_element else "N/A"
        
        current_time = datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
        
        return {
            "job_id": job_id,
            "url": clean_url,
            "company_name": company_name,
            "job_title": job_title,
            "location": location,
            "description": description,
            "html_file_path": html_filename,
            "scraped_at": current_time.isoformat(),
            "metadata_source": "basic"
        }
        
    except Exception as e:
        print(f"❌ Error in basic scraper for {job_url}: {e}")
        return None
    finally:
        page.close()
