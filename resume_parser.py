import re
import pdfplumber
from utils import extract_email, extract_experience_years, extract_skills

PRIMARY_SKILLS = [
    'PySpark', 'Databricks', 'AWS', 'Python', 'SQL', 
    'Apache Spark', 'Spark', 'Unity Catalog'
]

SECONDARY_SKILLS = [
    'Terraform', 'Docker', 'Git', 'Linux', 'Airflow',
    'ETL', 'Data Warehousing', 'Redshift', 'S3', 
    'Azure', 'Snowflake'
]

ADDITIONAL_SKILLS = [
    'JavaScript', 'C++', 'Java', 'Node.js', 'React',
    'Looker', 'Splunk', 'Tableau', 'Kafka', 'Flink'
]

EXPERTISE_KEYWORDS = [
    'pipeline', 'migration', 'optimization', 'consolidation',
    'cost reduction', 'performance', 'zero-downtime',
    'real-time', 'streaming', 'ETL', 'data warehouse'
]

def parse_resume(pdf_path: str) -> dict:
    """Parses a resume PDF and extracts key information."""
    print(f"📄 Parsing resume: {pdf_path}")
    
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    
    print(f"✅ Extracted {len(text)} characters")

    name = text.split('\n')[0].strip()
    print(f"✅ Found name: {name}")

    email = extract_email(text)
    print(f"✅ Found email: {email}")

    experience_years = extract_experience_years(text)
    print(f"✅ Experience: {experience_years} years")

    primary_skills = extract_skills(text, PRIMARY_SKILLS)
    secondary_skills = extract_skills(text, SECONDARY_SKILLS)
    additional_skills = extract_skills(text, ADDITIONAL_SKILLS)
    all_skills = list(set(primary_skills + secondary_skills + additional_skills))
    expertise_keywords = extract_skills(text, EXPERTISE_KEYWORDS)

    print("📊 Skills found:")
    print(f"   • Primary: {len(primary_skills)} skills")
    print(f"   • Secondary: {len(secondary_skills)} skills")
    print(f"   • Additional: {len(additional_skills)} skills")
    print(f"   • Total: {len(all_skills)} skills")
    print(f"✅ Expertise keywords: {len(expertise_keywords)} keywords")

    return {
        'name': name,
        'email': email,
        'experience_years': experience_years,
        'primary_skills': primary_skills,
        'secondary_skills': secondary_skills,
        'additional_skills': additional_skills,
        'all_skills': all_skills,
        'expertise_keywords': expertise_keywords
    }
