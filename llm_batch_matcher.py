# llm_batch_matcher.py
import logging
import json
import os
from typing import List, Dict, Optional
from openai import OpenAI
from datetime import datetime
import pytz
from rule_based_matcher import rule_based_match, batch_rule_based_match


_client = None

def get_openai_client():
    """
    Lazy initialization of OpenAI client.
    Only creates client when actually needed.
    """
    global _client
    
    if _client is None:
        api_key = os.getenv("OPENROUTER_API_KEY")
        
        if not api_key:
            raise ValueError(
                "OPENROUTER_API_KEY not found in environment variables. "
                "Please set it in your .env file."
            )
        
        _client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key
        )
        logging.info("✅ OpenRouter client initialized")
    
    return _client

SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata")

# JSON Schema for batch response - matches ALL your DB fields
BATCH_MATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "scores": {
                        "type": "object",
                        "properties": {
                            "technical": {"type": "number"},
                            "experience": {"type": "number"},
                            "culture": {"type": "number"},
                            "total": {"type": "number"}
                        },
                        "required": ["technical", "experience", "culture", "total"],
                        "additionalProperties": False
                    },
                    "classification": {
                        "type": "string",
                        "enum": ["EXCELLENT", "GOOD", "FAIR", "POOR"]
                    },
                    "matched_skills": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "skill_gaps": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "transferable_skills": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "strengths": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "weaknesses": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "recommendation": {
                        "type": "string",
                        "enum": ["APPLY", "CONSIDER", "SKIP"]
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Max 150 characters"
                    },
                    "deal_breakers": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "interview_tips": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "parsed_job_details": {
                        "type": "object",
                        "properties": {
                            "required_experience_years": {"type": ["number", "null"]},
                            "key_technologies": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "team_size": {"type": ["string", "null"]},
                            "role_level": {"type": ["string", "null"]}
                        },
                        "additionalProperties": False
                    }
                },
                "required": [
                    "job_id", "scores", "classification", "matched_skills",
                    "skill_gaps", "transferable_skills", "strengths", "weaknesses",
                    "recommendation", "reasoning", "deal_breakers", "interview_tips",
                    "parsed_job_details"
                ],
                "additionalProperties": False
            }
        }
    },
    "required": ["results"],
    "additionalProperties": False
}


def build_batch_prompt(jobs: List[Dict], resume_data: Dict) -> str:
    """
    Build a comprehensive prompt for batch job matching.
    """
    # Prepare jobs summary (limit description length to save tokens)
    jobs_data = []
    for job in jobs:
        jobs_data.append({
            "job_id": job.get("job_id"),
            "title": job.get("job_title"),
            "company": job.get("company_name"),
            "location": job.get("location"),
            "description": job.get("description", "")[:2500],  # Limit to 2500 chars
            "employment_type": job.get("employment_type"),
            "seniority_level": job.get("seniority_level"),
            "workplace_type": job.get("workplace_type")
        })
    
    prompt = f"""You are an expert job matching AI. Analyze {len(jobs)} job postings against this candidate's resume and return structured match analysis for EACH job.

**JOBS TO ANALYZE ({len(jobs)} total):**
{json.dumps(jobs_data, indent=2)}

**INSTRUCTIONS:**
For EACH job, provide a complete analysis with these exact fields:

1. **job_id**: The job's unique ID (CRITICAL - must match exactly)
2. **scores**: Object with technical (0-100), experience (0-100), culture (0-100), total (0-100)
3. **classification**: One of ["EXCELLENT", "GOOD", "FAIR", "POOR"]
4. **matched_skills**: Array of skills from resume that match job requirements
5. **skill_gaps**: Array of required skills candidate lacks
6. **transferable_skills**: Array of skills that could transfer to this role
7. **strengths**: Array of 2-4 key strengths for this specific role
8. **weaknesses**: Array of 2-4 key weaknesses or concerns
9. **recommendation**: One of ["APPLY", "CONSIDER", "SKIP"]
10. **reasoning**: One-liner summary (MAX 150 chars) - be concise!
11. **deal_breakers**: Array of critical mismatches (empty array if none)
12. **interview_tips**: Array of 2-3 specific tips for this role
13. **parsed_job_details**: Object with:
    - required_experience_years: number or null
    - key_technologies: array of strings
    - team_size: string or null
    - role_level: string or null

**CRITICAL RULES:**
- Return JSON ONLY, no markdown formatting
- Include ALL {len(jobs)} jobs in results array
- Keep reasoning under 150 characters
- Match job_id exactly from input
- Arrays can be empty but must exist
- No extra fields beyond schema

Return in this exact format:
{{
  "results": [
    {{ "job_id": "...", "scores": {{...}}, ... }},
    ...
  ]
}}"""
    
    return prompt


def batch_match_jobs(jobs: List[Dict], resume_data: Dict) -> Dict[str, Dict]:
    """
    Send multiple jobs in ONE LLM call and get structured matches.
    Falls back to rule-based matching if LLM fails.
    
    Args:
        jobs: List of job dictionaries (pre-filtered, usually 4-5 jobs)
        resume_data: Parsed resume data
    
    Returns:
        Dictionary mapping job_id to match data
    """
    if not jobs:
        logging.warning("No jobs provided for batch matching")
        return {}
    
    logging.info(f"🤖 Starting batch LLM analysis for {len(jobs)} jobs...")
    
    try:

        client = get_openai_client()
        # Build the prompt
        prompt = build_batch_prompt(jobs, resume_data)
        
        # Make single API call with JSON mode
        response = client.chat.completions.create(
            model="openai/gpt-4o-mini",  # Cost-effective model
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert job matching AI that returns valid JSON matching the exact schema provided."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            response_format={"type": "json_object"},  # Enable JSON mode
            temperature=0.3,
            max_tokens=4000  # Enough for 4-5 jobs
        )
        
        # Parse response
        response_text = response.choices[0].message.content
        batch_results = json.loads(response_text)
        
        # Validate structure
        if "results" not in batch_results:
            logging.error("Invalid response structure: missing 'results' key")
            raise ValueError("Invalid LLM response structure")
        
        # Map results to job_id
        results_map = {}
        for result in batch_results["results"]:
            job_id = result.get("job_id")
            if job_id:
                # Add metadata
                result["llm_analysis"] = True
                result["llm_model"] = "openai/gpt-4o-mini"
                result["matched_at"] = datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
                results_map[job_id] = result
            else:
                logging.warning("Result missing job_id, skipping")
        
        logging.info(f"✅ Batch LLM analysis completed: {len(results_map)}/{len(jobs)} jobs processed")

        print(results_map)
        
        # Check for missing jobs and use rule-based fallback
        input_job_ids = {job.get("job_id") for job in jobs}
        returned_job_ids = set(results_map.keys())
        missing = input_job_ids - returned_job_ids
        
        if missing:
            logging.warning(f"⚠️ LLM missed {len(missing)} jobs, using rule-based fallback")
            missing_jobs = [j for j in jobs if j.get("job_id") in missing]
            
            for job in missing_jobs:
                logging.info(f"🔧 Applying rule-based fallback for: {job.get('job_title')}")
                results_map[job.get("job_id")] = rule_based_match(job, resume_data)
        
        return results_map
    
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse LLM JSON response: {e}")
        logging.warning("🔧 Falling back to complete rule-based analysis")
        return batch_rule_based_match(jobs, resume_data)
    
    except Exception as e:
        logging.error(f"Batch LLM matching failed: {e}", exc_info=True)
        logging.warning("🔧 Falling back to complete rule-based analysis")
        return batch_rule_based_match(jobs, resume_data)
    

def create_fallback_match(job: Dict, reason: str = "LLM batch failed") -> Dict:
    """
    Create a fallback match response when LLM fails.
    Used for jobs that didn't get analyzed.
    """
    return {
        "job_id": job.get("job_id"),
        "scores": {
            "technical": 60,
            "experience": 60,
            "culture": 60,
            "total": 60
        },
        "classification": "FAIR",
        "matched_skills": [],
        "skill_gaps": [],
        "transferable_skills": [],
        "strengths": ["Fallback analysis - manual review recommended"],
        "weaknesses": ["Could not perform automated analysis"],
        "recommendation": "CONSIDER",
        "reasoning": "Automated analysis unavailable, requires manual review",
        "deal_breakers": [],
        "interview_tips": ["Review job description manually"],
        "parsed_job_details": {
            "required_experience_years": None,
            "key_technologies": [],
            "team_size": None,
            "role_level": None
        },
        "llm_analysis": False,
        "llm_model": None,
        "fallback_reason": reason,
        "matched_at": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
    }
