import os
from resume_parser import parse_resume
from rule_based_matcher import rule_based_match
from dotenv import load_dotenv
import json

load_dotenv()

RESUME_PATH = os.getenv("RESUME_PATH", "./Resume.pdf")
print(f"Parsing resume from: {RESUME_PATH}")

try:
    resume_data = parse_resume(RESUME_PATH)
    print("\n--- Resume Data ---")
    print(json.dumps(resume_data, indent=2, default=str))
    
    if not resume_data:
        print("❌ Resume parsing failed or returned empty data.")
    else:
        print(f"✅ Skills found: {len(resume_data.get('skills', []))}")
        print(f"✅ Tech Skills found: {len(resume_data.get('technical_skills', []))}")
        
        # Test with a dummy job
        dummy_job = {
            "job_id": "test_123",
            "job_title": "Data Engineer",
            "description": "We need a Data Engineer with Python, SQL, and AWS experience. 3+ years of experience required.",
            "company_name": "Test Corp"
        }
        
        print("\n--- Testing Matcher with Dummy Job ---")
        match = rule_based_match(dummy_job, resume_data)
        print(json.dumps(match, indent=2, default=str))

except Exception as e:
    print(f"❌ Error: {e}")
