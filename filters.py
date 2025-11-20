# filters.py - NEW FILE

# Job Title Keywords (case-insensitive matching)
JOB_TITLE_KEYWORDS = [
    "data engineer",
    "analytics engineer", 
    "data engineering",
    "analytics engineering",
    "etl engineer",
    "ml engineer"
]

# Must-Have Skill Keywords (at least 2 required)
REQUIRED_SKILLS = [
    "databricks",
    "pyspark",
    "spark",
    "sql",
    "python",
    "etl",
    "aws",
    "gcp",
    "azure"
]

# Minimum skill matches required
MIN_SKILL_MATCHES = 3

# filters.py (continued)

import re
from typing import Dict, List, Tuple

def check_keyword_match(text: str, keywords: List[str]) -> Tuple[bool, List[str]]:
    """
    Case-insensitive keyword matching.
    Returns (match_found, matched_keywords).
    """
    if not text:
        return False, []
    
    text_lower = text.lower()
    matched = []
    
    for keyword in keywords:
        if keyword.lower() in text_lower:
            matched.append(keyword)
    
    return len(matched) > 0, matched


def pre_filter_job(job: Dict) -> Dict:
    """
    Pre-filter a single job based on keyword criteria.
    Returns dict with filter results and reasons.
    """
    job_title = job.get('job_title', '')
    job_description = job.get('description', '')
    combined_text = f"{job_title} {job_description}"
    
    filter_result = {
        'job_id': job.get('job_id'),
        'job_title': job_title,
        'passed': False,
        'title_match': False,
        'skill_matches': [],
        'skill_count': 0,
        'exclude_match': False,
        'reason': ''
    }
    
    
    # Check 2: Job title match
    title_match, title_keywords = check_keyword_match(job_title, JOB_TITLE_KEYWORDS)
    filter_result['title_match'] = title_match
    
    # Check 3: Required skills match
    skills_match, matched_skills = check_keyword_match(combined_text, REQUIRED_SKILLS)
    filter_result['skill_matches'] = matched_skills
    filter_result['skill_count'] = len(matched_skills)
    
    # Decision: Pass if title matches OR sufficient skills found
    if title_match or filter_result['skill_count'] >= MIN_SKILL_MATCHES:
        filter_result['passed'] = True
        filter_result['reason'] = f"Title: {title_match}, Skills: {matched_skills}"
    else:
        filter_result['reason'] = f"Insufficient matches. Skills: {matched_skills}"
    
    return filter_result


def batch_pre_filter_jobs(jobs: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Filter all scraped jobs and return (passed_jobs, rejected_jobs).
    """
    passed = []
    rejected = []
    
    for job in jobs:
        filter_result = pre_filter_job(job)
        
        if filter_result['passed']:
            # Add filter metadata to job
            job['filter_metadata'] = {
                'skill_matches': filter_result['skill_matches'],
                'skill_count': filter_result['skill_count'],
                'title_match': filter_result['title_match']
            }
            passed.append(job)
        else:
            # Store rejection reason
            job['rejection_reason'] = filter_result['reason']
            rejected.append(job)
    
    return passed, rejected

