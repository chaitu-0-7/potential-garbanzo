# rule_based_matcher.py
"""
Rule-based job matching fallback system.
Uses keyword extraction, skill matching, and heuristics when LLM is unavailable.
"""

import re
import logging
from typing import Dict, List, Set, Tuple
from datetime import datetime
import pytz
from collections import Counter

# Common data engineering skills taxonomy
DATA_ENGINEERING_SKILLS = {
    'core': ['sql', 'python', 'etl', 'data pipeline', 'data warehousing', 'data modeling'],
    'big_data': ['spark', 'pyspark', 'hadoop', 'kafka', 'airflow', 'databricks'],
    'cloud': ['aws', 'azure', 'gcp', 'snowflake', 's3', 'redshift', 'bigquery'],
    'databases': ['postgresql', 'mysql', 'mongodb', 'redis', 'cassandra', 'dynamodb'],
    'tools': ['docker', 'kubernetes', 'git', 'jenkins', 'terraform', 'dbt'],
    'languages': ['java', 'scala', 'r', 'bash', 'shell scripting'],
    'ml': ['machine learning', 'tensorflow', 'scikit-learn', 'pandas', 'numpy']
}

# Flatten all skills for easy lookup
ALL_SKILLS = set()
for category in DATA_ENGINEERING_SKILLS.values():
    ALL_SKILLS.update(category)

# Experience level keywords
EXPERIENCE_KEYWORDS = {
    'entry': ['junior', 'entry level', '0-2 years', 'graduate', 'fresher', 'associate'],
    'mid': ['mid level', '2-5 years', '3-5 years', 'intermediate', 'engineer ii'],
    'senior': ['senior', '5+ years', '7+ years', 'lead', 'principal', 'staff', 'architect'],
    'expert': ['expert', '10+ years', 'director', 'head of', 'vp', 'chief']
}

# Negative signals (red flags)
RED_FLAGS = [
    'extensive travel required',
    'on-call 24/7',
    'must relocate',
    'commission only',
    'unpaid internship',
    'mandatory weekends'
]

SCHEDULER_TIMEZONE = "Asia/Kolkata"


def extract_skills_from_text(text: str, known_skills: Set[str] = ALL_SKILLS) -> List[str]:
    """
    Extract technical skills from text using keyword matching.
    
    Args:
        text: Job description or resume text
        known_skills: Set of known skill keywords
    
    Returns:
        List of matched skills
    """
    if not text:
        return []
    
    text_lower = text.lower()
    found_skills = []
    
    # Look for multi-word skills first (longer matches are more specific)
    sorted_skills = sorted(known_skills, key=len, reverse=True)
    
    for skill in sorted_skills:
        # Use word boundaries to avoid partial matches
        pattern = r'\b' + re.escape(skill.lower()) + r'\b'
        if re.search(pattern, text_lower):
            found_skills.append(skill)
    
    return list(set(found_skills))  # Remove duplicates


def calculate_skill_match_score(resume_skills: List[str], job_skills: List[str]) -> Tuple[float, List[str], List[str]]:
    """
    Calculate skill match percentage and identify matched/missing skills.
    
    Returns:
        (match_percentage, matched_skills, missing_skills)
    """
    if not job_skills:
        return 50.0, [], []  # Neutral score if no skills found
    
    resume_skills_set = set(s.lower() for s in resume_skills)
    job_skills_set = set(s.lower() for s in job_skills)
    
    matched = resume_skills_set.intersection(job_skills_set)
    missing = job_skills_set - resume_skills_set
    
    match_percentage = (len(matched) / len(job_skills_set)) * 100 if job_skills_set else 0
    
    return match_percentage, list(matched), list(missing)


def detect_experience_level(text: str) -> str:
    """
    Detect required experience level from job description.
    """
    text_lower = text.lower()
    
    for level, keywords in EXPERIENCE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text_lower:
                return level
    
    return 'mid'  # Default to mid-level


def extract_years_of_experience(text: str) -> int:
    """
    Extract required years of experience from text.
    """
    # Look for patterns like "5+ years", "3-5 years", "minimum 4 years"
    patterns = [
        r'(\d+)\+?\s*years',
        r'(\d+)-\d+\s*years',
        r'minimum\s+(\d+)\s*years',
        r'at least\s+(\d+)\s*years'
    ]
    
    text_lower = text.lower()
    years = []
    
    for pattern in patterns:
        matches = re.findall(pattern, text_lower)
        years.extend([int(m) for m in matches])
    
    return max(years) if years else 0


def detect_red_flags(text: str) -> List[str]:
    """
    Detect potential deal-breakers in job description.
    """
    text_lower = text.lower()
    found_flags = []
    
    for flag in RED_FLAGS:
        if flag in text_lower:
            found_flags.append(flag)
    
    return found_flags


def calculate_experience_score(resume_years: float, required_years: int, job_level: str) -> float:
    """
    Calculate experience match score.
    """
    if required_years == 0:
        # If no specific years mentioned, use level-based scoring
        level_requirements = {'entry': 1, 'mid': 3, 'senior': 6, 'expert': 10}
        required_years = level_requirements.get(job_level, 3)
    
    # Calculate percentage match
    if resume_years >= required_years:
        return 100.0
    elif resume_years >= required_years * 0.7:  # Within 70% of requirement
        return 80.0
    elif resume_years >= required_years * 0.5:  # Within 50% of requirement
        return 60.0
    else:
        return 40.0


def extract_key_technologies(job_description: str) -> List[str]:
    """
    Extract top technologies mentioned in job description.
    """
    skills = extract_skills_from_text(job_description)
    
    # Prioritize by category importance
    prioritized = []
    for category in ['big_data', 'cloud', 'core', 'databases', 'tools']:
        category_skills = DATA_ENGINEERING_SKILLS.get(category, [])
        for skill in skills:
            if skill.lower() in category_skills:
                prioritized.append(skill)
    
    return prioritized[:8]  # Top 8 technologies


def identify_transferable_skills(resume_skills: List[str], job_skills: List[str]) -> List[str]:
    """
    Identify skills from resume that could transfer to the role.
    """
    transferable = []
    
    resume_lower = [s.lower() for s in resume_skills]
    job_lower = [s.lower() for s in job_skills]
    
    # Check for related skills
    skill_families = [
        ['python', 'java', 'scala', 'r'],  # Programming languages
        ['sql', 'postgresql', 'mysql', 'oracle'],  # SQL databases
        ['aws', 'azure', 'gcp'],  # Cloud platforms
        ['spark', 'pyspark', 'hadoop'],  # Big data
        ['docker', 'kubernetes', 'containers']  # Containerization
    ]
    
    for family in skill_families:
        resume_in_family = [s for s in resume_lower if s in family]
        job_in_family = [s for s in job_lower if s in family]
        
        if resume_in_family and job_in_family:
            # Has related skill in same family
            for skill in resume_in_family:
                if skill not in job_in_family:
                    transferable.append(skill)
    
    return list(set(transferable))[:5]  # Top 5


def generate_strengths(matched_skills: List[str], experience_score: float, resume_data: Dict) -> List[str]:
    """
    Generate specific strengths based on analysis.
    """
    strengths = []
    
    if len(matched_skills) >= 5:
        strengths.append(f"Strong technical match with {len(matched_skills)} relevant skills")
    elif len(matched_skills) >= 3:
        strengths.append(f"Good skill alignment with {len(matched_skills)} key technologies")
    
    if experience_score >= 80:
        strengths.append("Experience level matches or exceeds requirements")
    
    # Check for high-value skills
    premium_skills = ['databricks', 'spark', 'airflow', 'kafka', 'snowflake']
    has_premium = [s for s in matched_skills if s.lower() in premium_skills]
    if has_premium:
        strengths.append(f"Expertise in high-demand tools: {', '.join(has_premium[:2])}")
    
    # Cloud experience
    cloud_skills = [s for s in matched_skills if s.lower() in ['aws', 'azure', 'gcp']]
    if cloud_skills:
        strengths.append(f"Cloud platform experience: {', '.join(cloud_skills)}")
    
    return strengths[:4] if strengths else ["Profile shows relevant technical background"]


def generate_weaknesses(missing_skills: List[str], experience_score: float, red_flags: List[str]) -> List[str]:
    """
    Generate specific weaknesses based on analysis.
    """
    weaknesses = []
    
    if len(missing_skills) >= 3:
        weaknesses.append(f"Missing {len(missing_skills)} required skills: {', '.join(missing_skills[:3])}")
    elif missing_skills:
        weaknesses.append(f"Skill gap in: {', '.join(missing_skills)}")
    
    if experience_score < 60:
        weaknesses.append("Experience level below requirement")
    
    if red_flags:
        weaknesses.append(f"Potential concerns: {red_flags[0]}")
    
    return weaknesses[:3] if weaknesses else ["Limited information to assess full fit"]


def generate_interview_tips(matched_skills: List[str], missing_skills: List[str]) -> List[str]:
    """
    Generate specific interview preparation tips.
    """
    tips = []
    
    if matched_skills:
        tips.append(f"Emphasize experience with {', '.join(matched_skills[:3])}")
    
    if missing_skills:
        tips.append(f"Prepare to discuss how you'd learn: {', '.join(missing_skills[:2])}")
    
    tips.append("Research company's data infrastructure and recent projects")
    
    return tips[:3]


def rule_based_match(job: Dict, resume_data: Dict) -> Dict:
    """
    Perform rule-based job matching using keyword extraction and heuristics.
    
    This is the fallback when LLM is unavailable.
    
    Args:
        job: Job dictionary with description, title, etc.
        resume_data: Parsed resume data
    
    Returns:
        Match data dictionary with all required fields
    """
    logging.info(f"🔧 Running rule-based analysis for: {job.get('job_title')}")
    
    job_description = job.get('description', '')
    job_title = job.get('job_title', '')
    combined_job_text = f"{job_title} {job_description}"
    
    # Extract resume skills
    resume_skills = resume_data.get('all_skills', [])
    if not resume_skills:
        resume_skills = resume_data.get('skills', [])
        resume_skills.extend(resume_data.get('primary_skills', []))
        resume_skills.extend(resume_data.get('secondary_skills', []))
    resume_skills = list(set(resume_skills))  # Deduplicate
    
    # Extract job skills
    job_skills = extract_skills_from_text(combined_job_text)
    
    # Calculate skill match
    skill_match_pct, matched_skills, missing_skills = calculate_skill_match_score(
        resume_skills, 
        job_skills
    )
    
    # Analyze experience
    required_years = extract_years_of_experience(job_description)
    job_level = detect_experience_level(job_description)
    resume_years = resume_data.get('total_experience_years', 0)
    experience_score = calculate_experience_score(resume_years, required_years, job_level)
    
    # Identify transferable skills
    transferable = identify_transferable_skills(resume_skills, job_skills)
    
    # Detect red flags
    red_flags = detect_red_flags(job_description)
    
    # Extract key technologies
    key_techs = extract_key_technologies(job_description)
    
    # Calculate scores
    technical_score = skill_match_pct
    culture_score = 80.0  # Boosted neutral score for fallback
    
    # Adjust technical score based on critical skills
    critical_skills = ['sql', 'python', 'etl', 'data pipeline']
    has_critical = [s for s in matched_skills if s.lower() in critical_skills]
    if len(has_critical) >= 2:
        technical_score = min(technical_score + 10, 100)
    
    total_score = (technical_score * 0.6) + (experience_score * 0.3) + (culture_score * 0.1)
    
    # Determine classification
    if total_score >= 80:
        classification = "EXCELLENT"
        recommendation = "APPLY"
    elif total_score >= 65:
        classification = "GOOD"
        recommendation = "APPLY"
    elif total_score >= 50:
        classification = "FAIR"
        recommendation = "CONSIDER"
    else:
        classification = "POOR"
        recommendation = "SKIP"
    
    # Adjust for red flags
    if red_flags:
        if classification == "EXCELLENT":
            classification = "GOOD"
        recommendation = "CONSIDER"
    
    # Generate insights
    strengths = generate_strengths(matched_skills, experience_score, resume_data)
    weaknesses = generate_weaknesses(missing_skills, experience_score, red_flags)
    interview_tips = generate_interview_tips(matched_skills, missing_skills)
    
    # Generate reasoning (max 150 chars)
    if total_score >= 70:
        reasoning = f"Strong fit: {len(matched_skills)} matching skills, {int(experience_score)}% exp match"
    elif total_score >= 50:
        reasoning = f"Moderate fit: {len(matched_skills)} skills match, consider skill gaps"
    else:
        reasoning = f"Weak fit: only {len(matched_skills)} skills match, significant gaps"
    
    # Ensure reasoning is under 150 chars
    reasoning = reasoning[:147] + "..." if len(reasoning) > 150 else reasoning
    
    # Build complete match result
    match_result = {
        "job_id": job.get("job_id"),
        "scores": {
            "technical": round(technical_score, 1),
            "experience": round(experience_score, 1),
            "culture": round(culture_score, 1),
            "total": round(total_score, 1)
        },
        "classification": classification,
        "matched_skills": matched_skills[:10],  # Limit to top 10
        "skill_gaps": missing_skills[:10],  # Limit to top 10
        "transferable_skills": transferable,
        "strengths": strengths,
        "weaknesses": weaknesses,
        "recommendation": recommendation,
        "reasoning": reasoning,
        "deal_breakers": red_flags,
        "interview_tips": interview_tips,
        "parsed_job_details": {
            "required_experience_years": required_years if required_years > 0 else None,
            "key_technologies": key_techs,
            "team_size": None,  # Can't extract without LLM
            "role_level": job_level
        },
        "llm_analysis": False,
        "llm_model": None,
        "fallback_reason": "Rule-based analysis (LLM unavailable)",
        "matched_at": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
    }
    
    logging.info(f"✅ Rule-based match complete: {total_score:.1f}% ({classification})")
    
    return match_result


def batch_rule_based_match(jobs: List[Dict], resume_data: Dict) -> Dict[str, Dict]:
    """
    Perform rule-based matching for multiple jobs.
    
    Args:
        jobs: List of job dictionaries
        resume_data: Parsed resume data
    
    Returns:
        Dictionary mapping job_id to match data
    """
    logging.info(f"🔧 Starting batch rule-based analysis for {len(jobs)} jobs...")
    
    results = {}
    
    for job in jobs:
        try:
            match_result = rule_based_match(job, resume_data)
            results[job.get("job_id")] = match_result
        except Exception as e:
            logging.error(f"Rule-based matching failed for {job.get('job_id')}: {e}")
            # Create minimal fallback
            results[job.get("job_id")] = {
                "job_id": job.get("job_id"),
                "scores": {"technical": 50, "experience": 50, "culture": 50, "total": 50},
                "classification": "FAIR",
                "matched_skills": [],
                "skill_gaps": [],
                "transferable_skills": [],
                "strengths": ["Analysis failed - manual review needed"],
                "weaknesses": ["Could not complete automated analysis"],
                "recommendation": "CONSIDER",
                "reasoning": "Automated analysis encountered errors",
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
                "fallback_reason": f"Rule-based analysis error: {str(e)[:50]}",
                "matched_at": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
            }
    
    logging.info(f"✅ Batch rule-based analysis completed: {len(results)} jobs processed")
    
    return results
