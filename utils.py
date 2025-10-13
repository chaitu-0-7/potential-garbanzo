import re

def extract_email(text: str) -> str:
    match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
    return match.group(0) if match else None

def extract_experience_years(text: str) -> int:
    # Simple regex to find patterns like "2+ years", "5 years", "3 years of experience"
    match = re.search(r'(\d+)\+?\s+years', text, re.IGNORECASE)
    return int(match.group(1)) if match else 0

def extract_skills(text: str, skills_list: list) -> list:
    found_skills = []
    for skill in skills_list:
        if re.search(r'\b' + re.escape(skill) + r'\b', text, re.IGNORECASE):
            found_skills.append(skill)
    return found_skills
