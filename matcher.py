import re
from utils import extract_skills, extract_experience_years
from resume_parser import PRIMARY_SKILLS, SECONDARY_SKILLS, EXPERTISE_KEYWORDS

def match_job(job: dict, resume: dict) -> dict:
    """Calculates a match score between a job and a resume."""
    
    job_description = job.get('description', '')
    if not job_description:
        return None

    print(f"🎯 Matching: {job.get('job_title')} at {job.get('company_name')}")

    # Extract skills from job description
    job_primary_skills = extract_skills(job_description, PRIMARY_SKILLS)
    job_secondary_skills = extract_skills(job_description, SECONDARY_SKILLS)
    job_expertise_keywords = extract_skills(job_description, EXPERTISE_KEYWORDS)
    print(f"  📊 Extracting skills from job description...")
    print(f"    - Primary: {job_primary_skills}")
    print(f"    - Secondary: {job_secondary_skills}")
    print(f"    - Expertise: {job_expertise_keywords}")

    # --- Technical Score (70 points) ---
    primary_matches = set(resume['primary_skills']) & set(job_primary_skills)
    primary_match_ratio = len(primary_matches) / len(job_primary_skills) if job_primary_skills else 0
    primary_score = primary_match_ratio * 40

    secondary_matches = set(resume['secondary_skills']) & set(job_secondary_skills)
    secondary_match_ratio = len(secondary_matches) / len(job_secondary_skills) if job_secondary_skills else 0
    secondary_score = secondary_match_ratio * 30

    technical_score = primary_score + secondary_score
    if primary_match_ratio == 1.0 and job_primary_skills:
        technical_score += 5 # Bonus
    if primary_match_ratio < 0.5:
        technical_score -= 10 # Penalty
    technical_score = min(max(technical_score, 0), 70) # Cap score between 0 and 70

    # --- Experience Score (10 points) ---
    required_years = extract_experience_years(job_description)
    candidate_years = resume.get('experience_years', 0)
    experience_score = 0
    if required_years > 0:
        if candidate_years >= required_years:
            experience_score = 10
        elif candidate_years >= required_years * 0.8:
            experience_score = 8
        elif candidate_years >= required_years * 0.6:
            experience_score = 5
        else:
            experience_score = 2
    else: # No specific requirement, give a neutral score
        experience_score = 5

    # --- Domain Score (20 points) ---
    domain_matches = set(resume['expertise_keywords']) & set(job_expertise_keywords)
    domain_match_ratio = len(domain_matches) / len(job_expertise_keywords) if job_expertise_keywords else 0
    domain_score = domain_match_ratio * 20

    # --- Total Score & Classification ---
    total_score = technical_score + experience_score + domain_score
    
    classification = "LOW"
    if total_score >= 90:
        classification = "EXCELLENT"
    elif total_score >= 75:
        classification = "STRONG"
    elif total_score >= 60:
        classification = "GOOD"

    all_job_skills = set(job_primary_skills + job_secondary_skills)
    skill_gaps = list(all_job_skills - set(resume['all_skills']))

    print("  💯 Calculating scores:")
    print(f"     • Technical: {technical_score:.1f} / 70 (Primary: {primary_match_ratio:.2f}, Secondary: {secondary_match_ratio:.2f})")
    print(f"     • Experience: {experience_score:.1f} / 10 (Required: {required_years}, Have: {candidate_years})")
    print(f"     • Domain: {domain_score:.1f} / 20 (Match Ratio: {domain_match_ratio:.2f})")
    print(f"  🎯 Total Score: {total_score:.1f}% ({classification})")
    print(f"  ✅ Matched: {', '.join(primary_matches | secondary_matches)}")
    print(f"  ⚠️  Gaps: {', '.join(skill_gaps)}")

    return {
        'scores': {
            'total': round(total_score, 1),
            'technical': round(technical_score, 1),
            'experience': round(experience_score, 1),
            'domain': round(domain_score, 1)
        },
        'matched_skills': list(primary_matches | secondary_matches),
        'skill_gaps': skill_gaps,
        'classification': classification
    }
