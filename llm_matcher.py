# llm_matcher.py (CORRECTED VERSION)
import os
import requests
from dotenv import load_dotenv
import json
import logging
import re
from typing import Dict, Optional, List
from datetime import datetime

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"

# Import the original matcher as fallback
from matcher import match_job as fallback_match_job
from llm_scraper import clean_job_description


# List of free models to try in order
FREE_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
    "meta-llama/llama-4-maverick:free",
]


def clean_resume_for_llm(resume_data: Dict) -> str:
    """
    Clean and format resume data for LLM, removing personal information.
    
    Removes:
    - Email addresses
    - Phone numbers
    - Full addresses
    - Personal URLs (if any)
    
    Returns formatted resume text suitable for LLM analysis.
    """
    # Get raw resume text if available
    resume_text = resume_data.get('raw_text', '')
    
    if not resume_text:
        # Construct resume text from structured data
        parts = []
        
        # Add summary
        if resume_data.get('summary'):
            parts.append(f"PROFESSIONAL SUMMARY:\n{resume_data['summary']}")
        
        # Add skills
        if resume_data.get('all_skills'):
            parts.append(f"\nSKILLS:\n{', '.join(resume_data['all_skills'])}")
        
        # Add experience years
        if resume_data.get('experience_years'):
            parts.append(f"\nTOTAL EXPERIENCE:\n{resume_data['experience_years']} years")
        
        # Add expertise keywords
        if resume_data.get('expertise_keywords'):
            parts.append(f"\nEXPERTISE:\n{', '.join(resume_data['expertise_keywords'])}")
        
        resume_text = '\n\n'.join(parts)
    
    # Remove email addresses
    resume_text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL REDACTED]', resume_text)
    
    # Remove phone numbers (various formats)
    resume_text = re.sub(r'(\+\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', '[PHONE REDACTED]', resume_text)
    resume_text = re.sub(r'\b\d{10}\b', '[PHONE REDACTED]', resume_text)
    
    # Remove URLs (keep LinkedIn profile structure but remove personal identifier)
    resume_text = re.sub(r'https?://(?:www\.)?linkedin\.com/in/[^\s]+', '[LINKEDIN PROFILE]', resume_text)
    resume_text = re.sub(r'https?://[^\s]+', '[URL REDACTED]', resume_text)
    
    # Remove potential addresses (simple pattern)
    resume_text = re.sub(r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct)', '[ADDRESS REDACTED]', resume_text)
    
    # Clean up excessive whitespace
    resume_text = re.sub(r'\n\s*\n', '\n\n', resume_text)
    resume_text = re.sub(r' +', ' ', resume_text)
    
    return resume_text.strip()


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


class LLMJobMatcher:
    """LLM-based job matching with multiple model fallback."""
    
    def __init__(self, models: List[str] = None):
        """
        Initialize with list of models to try.
        Will attempt each model in order until one succeeds.
        """
        self.models = models or FREE_MODELS
        self.headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:8000",
            "X-Title": "Job Scraper AI Matcher"
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
                    data=json.dumps(payload),  # Use data=json.dumps() not json=
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
        logging.error(f"All LLM models failed. Last error: {last_error}")
        return last_error or {"error": "all_models_failed"}
    
    def llm_parse_job_requirements(self, job: Dict) -> Optional[Dict]:
        """
        Use LLM to extract job requirements from description.
        Tries multiple models before giving up.
        """
        if not OPENROUTER_API_KEY:
            logging.warning("OPENROUTER_API_KEY not set. Skipping LLM parsing.")
            return None
        
        job_description = clean_job_description(job.get('description', ''))
        job_title = job.get('job_title', '')
        
        if not job_description:
            return None
        
        # Limit description to 2500 characters
        job_description = job_description[:2500]
        
        prompt = f"""Analyze this job posting and extract requirements. Respond ONLY with valid JSON, no other text.

Job Title: {job_title}

Job Description:
{job_description}

Return ONLY this JSON structure (no markdown, no explanation):
{{
    "min_experience_years": <number or null>,
    "max_experience_years": <number or null>,
    "must_have_skills": ["skill1", "skill2"],
    "nice_to_have_skills": ["skill1", "skill2"],
    "education_required": "<text or null>",
    "certifications_required": ["cert1"],
    "job_type": "<Full-time/Part-time/Contract/Internship or null>",
    "remote_policy": "<Remote/Hybrid/On-site or null>",
    "salary_range": "<text or null>",
    "key_responsibilities": ["resp1", "resp2"],
    "red_flags": ["flag1"],
    "growth_potential": "<text>"
}}"""

        messages = [
            {
                "role": "system", 
                "content": "You extract structured data from job postings. Respond ONLY with valid JSON. No markdown formatting, no explanations, just the JSON object."
            },
            {
                "role": "user", 
                "content": prompt
            }
        ]
        
        result = self._call_llm(messages, retry_models=True)
        
        if "error" in result:
            logging.error(f"LLM job parsing failed: {result['error']}")
            return None
        
        return result
    
    def llm_match_resume_to_job(self, job: Dict, full_resume_text: str, parsed_requirements: Dict) -> Optional[Dict]:
        """
        Use LLM to match resume to job with detailed scoring.
        Tries multiple models before giving up.
        """
        if not OPENROUTER_API_KEY:
            logging.warning("OPENROUTER_API_KEY not set. Skipping LLM matching.")
            return None
        
        # Clean and limit resume text
        resume_text = full_resume_text[:3000]
        
        # Prepare job summary
        job_title = job.get('job_title', '')
        job_description = clean_job_description(job.get('description', ''))[:2000]
        company_name = job.get('company_name', '')
        
        # Get parsed requirements
        min_exp = parsed_requirements.get('min_experience_years', 0) if parsed_requirements else 0
        must_have = parsed_requirements.get('must_have_skills', []) if parsed_requirements else []
        nice_to_have = parsed_requirements.get('nice_to_have_skills', []) if parsed_requirements else []
        
        prompt = f"""Evaluate candidate fit for this job. Respond ONLY with valid JSON.

JOB:
Company: {company_name}
Title: {job_title}
Required Experience: {min_exp} years
Must-Have: {', '.join(must_have)}
Preferred: {', '.join(nice_to_have)}

Job Description:
{job_description}

RESUME:
{resume_text}

Return ONLY this JSON (no markdown, no explanation):
{{
    "overall_match_score": <0-100 number>,
    "technical_skill_score": <0-70 number>,
    "experience_level_score": <0-10 number>,
    "domain_match_score": <0-20 number>,
    "matched_skills": ["skill1", "skill2"],
    "missing_critical_skills": ["skill1"],
    "transferable_skills": ["skill1"],
    "classification": "<EXCELLENT/STRONG/GOOD/FAIR/POOR>",
    "recommendation": "<APPLY_IMMEDIATELY/APPLY/CONSIDER/SKIP>",
    "reasoning": "<brief explanation>",
    "strengths": ["strength1", "strength2", "strength3"],
    "weaknesses": ["weakness1", "weakness2"],
    "deal_breakers": ["blocker1"],
    "interview_tips": ["tip1", "tip2"]
}}

Scoring:
- Technical (0-70): Skill match quality
- Experience (0-10): Years + relevance
- Domain (0-20): Industry fit
- Classification: EXCELLENT (90-100), STRONG (75-89), GOOD (60-74), FAIR (40-59), POOR (0-39)"""

        messages = [
            {
                "role": "system",
                "content": "You are a career advisor. Assess candidate-job fit objectively. Respond ONLY with valid JSON, no other text."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        result = self._call_llm(messages, retry_models=True)
        
        if "error" in result:
            logging.error(f"LLM matching failed: {result['error']}")
            return None
        
        return result


def llm_match_job(job: dict, resume: dict) -> dict:
    """
    Enhanced job matching using LLM with multiple model fallback.
    
    Tries free models in this order:
    1. meta-llama/llama-3.3-70b-instruct:free
    2. deepseek/deepseek-r1-distill-llama-70b:free
    3. meta-llama/llama-4-maverick:free
    
    Falls back to rule-based matcher only if all LLMs fail.
    """
    print(f"🤖 LLM Matching: {job.get('job_title')} at {job.get('company_name')}")
    
    try:
        matcher = LLMJobMatcher(models=FREE_MODELS)
        
        # Step 1: Clean resume for LLM (remove PII)
        print("  🧹 Cleaning resume text (removing personal information)...")
        cleaned_resume_text = clean_resume_for_llm(resume)
        print(f"    ✓ Resume length: {len(cleaned_resume_text)} characters")
        
        # Step 2: Parse job requirements with LLM
        print(f"  📊 Parsing job requirements (trying {len(FREE_MODELS)} models)...")
        parsed_requirements = matcher.llm_parse_job_requirements(job)
        
        if not parsed_requirements or "error" in parsed_requirements:
            print("  ⚠️  All LLM models failed for parsing, falling back to rule-based matcher")
            result = fallback_match_job(job, resume)
            result['llm_analysis'] = False
            result['fallback_reason'] = 'llm_parsing_failed_all_models'
            return result
        
        llm_model_used = parsed_requirements.pop('_llm_model_used', 'unknown')
        print(f"    ✓ Success with model: {llm_model_used}")
        print(f"    ✓ Min Experience: {parsed_requirements.get('min_experience_years', 'N/A')} years")
        print(f"    ✓ Must-Have Skills: {len(parsed_requirements.get('must_have_skills', []))} identified")
        
        # Step 3: Check experience requirement
        min_exp = parsed_requirements.get('min_experience_years')
        candidate_exp = resume.get('experience_years', 0)
        
        if min_exp and min_exp > 4:
            print(f"  ❌ Auto-rejecting: Requires {min_exp} years (threshold: 4, you have: {candidate_exp})")
            return {
                "scores": {"total": 0, "technical": 0, "experience": 0, "domain": 0},
                "classification": "POOR",
                "matched_skills": [],
                "skill_gaps": parsed_requirements.get('must_have_skills', []),
                "skip_reason": f"Requires {min_exp} years experience (you have {candidate_exp})",
                "parsed_job_details": parsed_requirements,
                "llm_analysis": True,
                "llm_model": llm_model_used,
                "recommendation": "SKIP",
                "reasoning": f"Job requires {min_exp} years of experience which exceeds your {candidate_exp} years."
            }
        
        # Step 4: LLM matching with full resume
        print(f"  🧠 Matching resume to job (trying {len(FREE_MODELS)} models)...")
        match_result = matcher.llm_match_resume_to_job(job, cleaned_resume_text, parsed_requirements)
        
        if not match_result or "error" in match_result:
            print("  ⚠️  All LLM models failed for matching, falling back to rule-based matcher")
            result = fallback_match_job(job, resume)
            result['llm_analysis'] = False
            result['fallback_reason'] = 'llm_matching_failed_all_models'
            result['parsed_job_details'] = parsed_requirements
            return result
        
        llm_model_used = match_result.pop('_llm_model_used', llm_model_used)
        print(f"    ✓ Success with model: {llm_model_used}")
        
        # Step 5: Format result
        final_result = {
            "scores": {
                "total": round(match_result.get('overall_match_score', 0), 1),
                "technical": round(match_result.get('technical_skill_score', 0), 1),
                "experience": round(match_result.get('experience_level_score', 0), 1),
                "domain": round(match_result.get('domain_match_score', 0), 1)
            },
            "classification": match_result.get('classification', 'FAIR'),
            "matched_skills": match_result.get('matched_skills', []),
            "skill_gaps": match_result.get('missing_critical_skills', []),
            "transferable_skills": match_result.get('transferable_skills', []),
            "strengths": match_result.get('strengths', []),
            "weaknesses": match_result.get('weaknesses', []),
            "recommendation": match_result.get('recommendation', 'CONSIDER'),
            "reasoning": match_result.get('reasoning', ''),
            "deal_breakers": match_result.get('deal_breakers', []),
            "interview_tips": match_result.get('interview_tips', []),
            "parsed_job_details": parsed_requirements,
            "llm_analysis": True,
            "llm_model": llm_model_used
        }
        
        print(f"  ✅ LLM Match Complete:")
        print(f"     • Total Score: {final_result['scores']['total']}% ({final_result['classification']})")
        print(f"     • Recommendation: {final_result['recommendation']}")
        print(f"     • Reasoning: {final_result['reasoning'][:100]}...")
        print(f"     • Model Used: {llm_model_used}")
        
        return final_result
        
    except Exception as e:
        print(f"  ❌ LLM matching exception: {e}")
        print("  🔄 Falling back to rule-based matcher")
        logging.error(f"LLM matching exception: {e}", exc_info=True)
        
        result = fallback_match_job(job, resume)
        result['llm_analysis'] = False
        result['fallback_reason'] = 'exception'
        result['error'] = str(e)
        return result


if __name__ == "__main__":
    # Test the multi-model LLM matcher
    logging.basicConfig(level=logging.INFO)
    
    test_job = {
        "job_id": "test123",
        "job_title": "Data Engineer",
        "company_name": "Tech Corp",
        "description": """We need a Data Engineer with 2-3 years experience in AWS and Python.
        
Must have: Python, SQL, AWS
Nice to have: Databricks, Docker

Responsibilities:
- Build data pipelines
- Work with data team""",
        "location": "Hyderabad"
    }
    
    test_resume = {
        "raw_text": """Data Engineer with 2 years experience.
Skills: Python, SQL, AWS, Databricks, PySpark
Experience at HP building ETL pipelines.""",
        "all_skills": ["Python", "SQL", "AWS", "Databricks", "PySpark"],
        "experience_years": 2,
    }
    
    print("\n" + "="*70)
    print("TESTING MULTI-MODEL LLM MATCHER")
    print("="*70)
    
    result = llm_match_job(test_job, test_resume)
    
    print("\n" + "="*70)
    print("FINAL RESULT")
    print("="*70)
    print(json.dumps(result, indent=2))
