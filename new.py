# new.py (UPDATED - Enhanced DB storage)
import logging
import re
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Import your project modules
from scraper import scrape_jobs
from llm_matcher import llm_match_job as match_job
from database import MongoDB
from discord_notifier import send_discord_notification,send_summary_notification
from resume_parser import parse_resume

# Configure logging
from logging_config import setup_logging
setup_logging()

# --- CONFIGURATION ---
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"
SCHEDULER_START_HOUR = int(os.getenv("SCHEDULER_START_HOUR", 7))
SCHEDULER_END_HOUR = int(os.getenv("SCHEDULER_END_HOUR", 23))
SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata")
SCRAPE_MAX_JOBS = int(os.getenv("SCRAPE_MAX_JOBS", 30))
MIN_MATCH_SCORE = int(os.getenv("MIN_MATCH_SCORE", 60))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
LINKEDIN_URL = os.getenv("LINKEDIN_URL")
RESUME_PATH = os.getenv("RESUME_PATH")


def get_linkedin_url(time_filter_seconds):
    """Modifies the LinkedIn URL to search for jobs posted in the last N seconds."""
    base_url = LINKEDIN_URL
    if "f_TPR" in base_url:
        # Replace existing f_TPR parameter
        return re.sub(r"f_TPR=r\d+", f"f_TPR=r{time_filter_seconds}", base_url)
    else:
        # Add f_TPR parameter
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}f_TPR=r{time_filter_seconds}"


# new.py (UPDATED scrape_and_match_task function)

def scrape_and_match_task(is_morning_run=False, is_hourly_run=False, is_startup_run=False):
    """The main task that runs on a schedule with summary notification."""
    import time as time_module
    start_time = time_module.time()
    
    logging.info("========================================")
    logging.info("🤖 AUTOMATED TASK STARTED")
    logging.info(f"Time: {datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    if is_startup_run:
        run_type = "startup"
        logging.info("Mode: Startup Run (Immediate)")
    elif is_morning_run:
        run_type = "morning_catchup"
        logging.info("Mode: Morning Catch-up")
    elif is_hourly_run:
        run_type = "hourly"
        logging.info("Mode: Hourly")
    else:
        run_type = "manual"
        logging.info("Mode: Manual")
    
    logging.info("========================================")
    
    # Initialize statistics tracking
    stats = {
        'run_type': run_type,
        'timestamp': datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)),
        'jobs_scraped': 0,
        'new_jobs': 0,
        'already_notified': 0,
        'matches_found': 0,
        'notifications_sent': 0,
        'below_threshold': 0,
        'llm_successes': 0,
        'llm_fallbacks': 0,
        'errors': [],
        'top_matches': [],
        'status': 'success'
    }
    db_instance = MongoDB()

    try:
            jobs_collection = db_instance.get_collection("jobs")
            notifications_collection = db_instance.get_collection("notifications")
            matches_collection = db_instance.get_collection("matches")

            # Determine URL
            if is_startup_run:
                url_to_scrape = get_linkedin_url(86400)
                logging.info("Using 24-hour lookback for startup run")
            elif is_morning_run:
                url_to_scrape = get_linkedin_url(43200)
                logging.info("Using 12-hour lookback for morning run")
            elif is_hourly_run:
                url_to_scrape = get_linkedin_url(3600)
                logging.info("Using 1-hour lookback for hourly run")
            else:
                url_to_scrape = LINKEDIN_URL
                logging.info("Using default URL")
            
            logging.info(f"Scraping URL: {url_to_scrape}")

            # Scrape jobs
            try:
                scraped_jobs, filename = scrape_jobs(url_to_scrape, SCRAPE_MAX_JOBS)
                stats['jobs_scraped'] = len(scraped_jobs) if scraped_jobs else 0
            except Exception as e:
                logging.error(f"Scraping failed: {e}", exc_info=True)
                stats['errors'].append(f"Scraping failed: {str(e)[:100]}")
                stats['status'] = 'failed'
                scraped_jobs = []
            
            if not scraped_jobs:
                logging.info("No jobs found from scraper.")
                stats['status'] = 'partial' if stats['errors'] else 'success'
                end_time = time_module.time()
                stats['execution_time_seconds'] = end_time - start_time
                send_summary_notification(stats)
                return

            logging.info(f"Scraped {len(scraped_jobs)} jobs from LinkedIn")

            # Double-check against notifications (safety check)
            truly_new_jobs = []
            already_notified_count = 0
            
            for job in scraped_jobs:
                job_id = job.get('job_id')
                already_notified = notifications_collection.find_one({"job_id": job_id})
                
                if not already_notified:
                    truly_new_jobs.append(job)
                    existing_job = jobs_collection.find_one({"job_id": job_id})
                    
                    if not existing_job:
                        job_document = {
                            "job_id": job_id,
                            "url": job.get('url'),
                            "company_name": job.get('company_name'),
                            "job_title": job.get('job_title'),
                            "location": job.get('location'),
                            "description": job.get('description'),
                            "html_file_path": job.get('html_file_path'),
                            "time_posted_text": job.get('time_posted_text'),
                            "time_posted_hours": job.get('time_posted_hours'),
                            "posted_at": job.get('posted_at'),
                            "applicant_count": job.get('applicant_count'),
                            "applicant_count_text": job.get('applicant_count_text'),
                            "employment_type": job.get('employment_type'),
                            "seniority_level": job.get('seniority_level'),
                            "workplace_type": job.get('workplace_type'),
                            "metadata_source": job.get('metadata_source'),
                            "metadata_model": job.get('metadata_model'),
                            "scraped_at": job.get('scraped_at'),
                            "first_seen": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
                        }
                        jobs_collection.insert_one(job_document)
                else:
                    already_notified_count += 1
            
            stats['new_jobs'] = len(truly_new_jobs)
            stats['already_notified'] = already_notified_count
            
            logging.info(f"💾 Found {len(truly_new_jobs)} new jobs. Skipped {already_notified_count} already notified.")

            if not truly_new_jobs:
                logging.info("No new jobs to process.")
                end_time = time_module.time()
                stats['execution_time_seconds'] = end_time - start_time
                send_summary_notification(stats)
                return

            # Parse resume
            resume_data = parse_resume(RESUME_PATH)
            if not resume_data:
                logging.error("Failed to parse resume.")
                stats['errors'].append("Resume parsing failed")
                stats['status'] = 'failed'
                end_time = time_module.time()
                stats['execution_time_seconds'] = end_time - start_time
                send_summary_notification(stats)
                return

            # Match and notify
            top_matches = []
            
            for job in truly_new_jobs:
                job_id = job.get('job_id')
                
                try:
                    match_data = match_job(job, resume_data)
                    
                    if match_data.get('llm_analysis'):
                        stats['llm_successes'] += 1
                    else:
                        stats['llm_fallbacks'] += 1
                    
                    match_score = match_data.get('scores', {}).get('total', 0)
                    
                    if match_data and match_score >= MIN_MATCH_SCORE:
                        stats['matches_found'] += 1
                        
                        top_matches.append({
                            'job_title': job.get('job_title'),
                            'company': job.get('company_name'),
                            'score': match_score
                        })
                        
                        # Save match
                        match_document = {
                            "job_id": job_id,
                            "job_title": job.get('job_title'),
                            "company_name": job.get('company_name'),
                            "scores": match_data.get('scores', {}),
                            "classification": match_data.get('classification'),
                            "matched_skills": match_data.get('matched_skills', []),
                            "skill_gaps": match_data.get('skill_gaps', []),
                            "transferable_skills": match_data.get('transferable_skills', []),
                            "strengths": match_data.get('strengths', []),
                            "weaknesses": match_data.get('weaknesses', []),
                            "recommendation": match_data.get('recommendation'),
                            "reasoning": match_data.get('reasoning'),
                            "deal_breakers": match_data.get('deal_breakers', []),
                            "interview_tips": match_data.get('interview_tips', []),
                            "parsed_job_details": match_data.get('parsed_job_details', {}),
                            "llm_analysis": match_data.get('llm_analysis', False),
                            "llm_model": match_data.get('llm_model'),
                            "fallback_reason": match_data.get('fallback_reason'),
                            "matched_at": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
                        }
                        matches_collection.update_one({"job_id": job_id}, {"$set": match_document}, upsert=True)
                        
                        # Send notification
                        notification_payload = {"job": job, "match": match_data}
                        status = send_discord_notification(notification_payload)
                        
                        # Record notification
                        notification_document = {
                            "job_id": job_id,
                            "job_title": job.get('job_title', 'Unknown'),
                            "company": job.get('company_name', 'Unknown'),
                            "location": job.get('location', 'Unknown'),
                            "match_score": match_score,
                            "classification": match_data.get('classification'),
                            "recommendation": match_data.get('recommendation'),
                            "llm_analysis": match_data.get('llm_analysis', False),
                            "llm_model": match_data.get('llm_model'),
                            "strengths": match_data.get('strengths', []),
                            "weaknesses": match_data.get('weaknesses', []),
                            "reasoning": match_data.get('reasoning'),
                            "posted_at": job.get('posted_at'),
                            "time_posted_hours": job.get('time_posted_hours'),
                            "applicant_count": job.get('applicant_count'),
                            "employment_type": job.get('employment_type'),
                            "workplace_type": job.get('workplace_type'),
                            "seniority_level": job.get('seniority_level'),
                            "status": status,
                            "timestamp": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)),
                            "run_type": run_type
                        }
                        notifications_collection.insert_one(notification_document)
                        
                        if status == 'success':
                            stats['notifications_sent'] += 1
                            logging.info(f"✅ Sent: {job.get('job_title')} ({match_score:.1f}%)")
                        else:
                            logging.warning(f"⚠️ Failed: {job.get('job_title')}")
                            stats['errors'].append(f"Notification failed: {job.get('job_title')}")
                    else:
                        stats['below_threshold'] += 1
                        notification_document = {
                            "job_id": job_id,
                            "job_title": job.get('job_title', 'Unknown'),
                            "company": job.get('company_name', 'Unknown'),
                            "location": job.get('location', 'Unknown'),
                            "match_score": match_score,
                            "classification": match_data.get('classification') if match_data else 'N/A',
                            "status": "skipped_low_score",
                            "skip_reason": match_data.get('skip_reason') if match_data else 'Below threshold',
                            "timestamp": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)),
                            "run_type": run_type,
                            "llm_analysis": match_data.get('llm_analysis', False) if match_data else False,
                            "time_posted_hours": job.get('time_posted_hours'),
                            "applicant_count": job.get('applicant_count'),
                        }
                        notifications_collection.insert_one(notification_document)
                        
                except Exception as e:
                    logging.error(f"Error processing job {job_id}: {e}", exc_info=True)
                    stats['errors'].append(f"Error: {job.get('job_title', job_id)[:50]}")
                    
                    notifications_collection.insert_one({
                        "job_id": job_id,
                        "job_title": job.get('job_title', 'Unknown'),
                        "company": job.get('company_name', 'Unknown'),
                        "status": "error",
                        "error_message": str(e),
                        "timestamp": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)),
                        "run_type": run_type
                    })
                    continue
            
            top_matches.sort(key=lambda x: x['score'], reverse=True)
            stats['top_matches'] = top_matches[:5]
            
            logging.info(f"📊 Results: {stats['matches_found']} matches, {stats['notifications_sent']} sent, {stats['below_threshold']} below threshold")
            
            if stats['errors']:
                stats['status'] = 'partial'
            else:
                stats['status'] = 'success'
        
        # MongoDB connection automatically closed here

    except Exception as e:
        logging.error(f"Critical error: {e}", exc_info=True)
        stats['errors'].append(f"Critical error: {str(e)[:100]}")
        stats['status'] = 'failed'
    finally:
        if db_instance:
            db_instance.close_connection()
        end_time = time_module.time()
        stats['execution_time_seconds'] = end_time - start_time
        
        if not is_github_actions():
            current_hour = datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)).hour
            if SCHEDULER_START_HOUR <= current_hour < SCHEDULER_END_HOUR:
                next_hour = current_hour + 1
                if next_hour <= SCHEDULER_END_HOUR:
                    stats['next_run_time'] = f"Today at {next_hour}:00"
                else:
                    stats['next_run_time'] = f"Tomorrow at {SCHEDULER_START_HOUR}:00"
            else:
                stats['next_run_time'] = f"Tomorrow at {SCHEDULER_START_HOUR}:00"
        
        logging.info("📤 Sending summary notification...")
        send_summary_notification(stats)
        
        logging.info("✅ TASK COMPLETED")
        logging.info("========================================\n")

def setup_scheduler():
    """Configures and starts the scheduler."""
    if not SCHEDULER_ENABLED:
        logging.info("Scheduler is disabled via environment variable.")
        return None

    scheduler = BackgroundScheduler(timezone=SCHEDULER_TIMEZONE)

    # Morning catch-up task at 7:00 AM
    scheduler.add_job(
        lambda: scrape_and_match_task(is_morning_run=True),
        trigger=CronTrigger(
            hour=SCHEDULER_START_HOUR,
            minute=0,
            timezone=SCHEDULER_TIMEZONE
        ),
        id='morning_scrape_and_match',
        name='Morning Catch-up (12h lookback)',
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300
    )

    # Hourly tasks from 8 AM to 11 PM
    scheduler.add_job(
        lambda: scrape_and_match_task(is_hourly_run=True),
        trigger=CronTrigger(
            hour=f'{SCHEDULER_START_HOUR+1}-{SCHEDULER_END_HOUR}',
            minute=0,
            timezone=SCHEDULER_TIMEZONE
        ),
        id='hourly_scrape_and_match',
        name='Hourly Scrape (1h lookback)',
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300
    )

    logging.info("🤖 Starting job scheduler...")
    scheduler.start()
    logging.info("✅ Scheduler started successfully.")
    logging.info("📅 Scheduled jobs:")
    for job in scheduler.get_jobs():
        logging.info(f"   • {job.name} (ID: {job.id})")
        logging.info(f"     Next run: {job.next_run_time}")

    return scheduler


def create_indexes(db_instance):
    """
    Create indexes for better analytics performance.
    Run this once to optimize database queries.
    """
    logging.info("📊 Creating database indexes for analytics...")
    
    jobs_collection = db_instance.get_collection("jobs")
    notifications_collection = db_instance.get_collection("notifications")
    matches_collection = db_instance.get_collection("matches")
    
    # Jobs collection indexes
    jobs_collection.create_index("job_id", unique=True)
    jobs_collection.create_index("scraped_at")
    jobs_collection.create_index("time_posted_hours")
    jobs_collection.create_index("applicant_count")
    jobs_collection.create_index("employment_type")
    jobs_collection.create_index("workplace_type")
    jobs_collection.create_index("metadata_source")
    
    # Notifications collection indexes
    notifications_collection.create_index("job_id", unique=True)
    notifications_collection.create_index("timestamp")
    notifications_collection.create_index("status")
    notifications_collection.create_index("match_score")
    notifications_collection.create_index("run_type")
    notifications_collection.create_index("llm_analysis")
    
    # Matches collection indexes
    matches_collection.create_index("job_id", unique=True)
    matches_collection.create_index([("scores.total", -1)])  # Descending order
    matches_collection.create_index("classification")
    matches_collection.create_index("recommendation")
    matches_collection.create_index("llm_analysis")
    matches_collection.create_index("matched_at")
    
    logging.info("✅ Indexes created successfully")


# new.py (ADD THIS FUNCTION)
def is_github_actions():
    """Check if running in GitHub Actions environment."""
    return os.getenv('GITHUB_ACTIONS') == 'true'

# Then update __main__ section:
if __name__ == "__main__":
    logging.info("="*60)
    logging.info("🚀 JOB SCRAPER SCHEDULER STARTING (LLM-ENHANCED)")
    logging.info("="*60)
    
    # Check if running in GitHub Actions
    if is_github_actions():
        logging.info("Running in GitHub Actions mode - single execution")
        # GitHub Actions will call the function directly via workflow
    else:
        # Local mode - run with scheduler
        logging.info("Running in local mode - with scheduler")
        
        # Create indexes
        # try:
        #     # db = MongoDB()
        #     # create_indexes(db)
        #     # db.close_connection()
        # except Exception as e:
        #     logging.warning(f"Index creation skipped: {e}")
        
        # Run immediately on startup
        logging.info("\n⚡ Running IMMEDIATE startup scan...")
        try:
            scrape_and_match_task(is_hourly_run=True)
        except Exception as e:
            logging.error(f"Error during startup run: {e}", exc_info=True)
        
        # Setup and start the scheduler
        scheduler = setup_scheduler()
        
        if scheduler:
            logging.info("\n💤 Scheduler is now running in background. Press Ctrl+C to exit.")
            try:
                while True:
                    time.sleep(60)
            except (KeyboardInterrupt, SystemExit):
                logging.info("\n🛑 Shutdown signal received...")
                scheduler.shutdown()
                logging.info("✅ Scheduler stopped gracefully.")
        else:
            logging.info("No scheduler to run.")

