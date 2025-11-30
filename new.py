# new.py (UPDATED - Enhanced DB storage & UX)
import logging
import re
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from dotenv import load_dotenv
import os
import time
import sys

# Rich Console
from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

# Custom theme for rich
custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
    "highlight": "magenta"
})
console = Console(theme=custom_theme)

from llm_batch_matcher import batch_match_jobs, create_fallback_match

# Load environment variables
load_dotenv()

# Import your project modules
from filters import batch_pre_filter_jobs
from scraper import scrape_jobs
from llm_matcher import llm_match_job as match_job
from database import MongoDB
from discord_notifier import send_discord_notification, send_summary_notification
from resume_parser import parse_resume

# Configure logging to file only (Rich handles console)
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create handlers
file_handler = logging.FileHandler(os.path.join(log_dir, "scheduler.log"))
error_handler = logging.FileHandler(os.path.join(log_dir, "errors.log"))
error_handler.setLevel(logging.ERROR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, error_handler]
)

# --- CONFIGURATION ---
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"
SCHEDULER_START_HOUR = int(os.getenv("SCHEDULER_START_HOUR", 7))
SCHEDULER_END_HOUR = int(os.getenv("SCHEDULER_END_HOUR", 23))
SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata")
SCRAPE_MAX_JOBS = int(os.getenv("SCRAPE_MAX_JOBS", 30))
MIN_MATCH_SCORE = int(os.getenv("MIN_MATCH_SCORE", 50))
FORCE_NOTIFY = os.getenv("FORCE_NOTIFY", "false").lower() == "true"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
LINKEDIN_URL = os.getenv("LINKEDIN_URL")
RESUME_PATH = os.getenv("RESUME_PATH")


def validate_config():
    """Validates essential configuration variables."""
    missing = []
    if not DISCORD_WEBHOOK_URL:
        missing.append("DISCORD_WEBHOOK_URL")
    if not LINKEDIN_URL:
        missing.append("LINKEDIN_URL")
    if not RESUME_PATH or not os.path.exists(RESUME_PATH):
        missing.append(f"RESUME_PATH (File not found: {RESUME_PATH})")
    
    if missing:
        console.print(Panel(f"[bold red]Configuration Error![/]\nMissing or invalid variables:\n" + "\n".join([f"- {m}" for m in missing]), title="Startup Check", border_style="red"))
        sys.exit(1)
    
    if FORCE_NOTIFY:
        console.print(Panel("[bold yellow]⚠️  FORCE_NOTIFY is ENABLED[/]\nAll jobs will be sent to Discord regardless of match score.", border_style="yellow"))
    
    console.print("[success]✓ Configuration validated[/]")


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
    
    console.rule("[bold cyan]🤖 AUTOMATED TASK STARTED[/]")
    logging.info("AUTOMATED TASK STARTED")
    
    current_time_str = datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)).strftime('%Y-%m-%d %H:%M:%S %Z')
    console.print(f"Time: [highlight]{current_time_str}[/]")
    
    if is_startup_run:
        run_type = "startup"
        console.print("Mode: [bold yellow]Startup Run (Immediate)[/]")
    elif is_morning_run:
        run_type = "morning_catchup"
        console.print("Mode: [bold yellow]Morning Catch-up[/]")
    elif is_hourly_run:
        run_type = "hourly"
        console.print("Mode: [bold yellow]Hourly[/]")
    else:
        run_type = "manual"
        console.print("Mode: [bold yellow]Manual[/]")
    
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
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True
        ) as progress:
            
            task_db = progress.add_task("[cyan]Connecting to Database...", total=None)
            jobs_collection = db_instance.get_collection("jobs")
            notifications_collection = db_instance.get_collection("notifications")
            matches_collection = db_instance.get_collection("matches")
            progress.update(task_db, completed=True)

            # Determine URL
            if is_startup_run:
                url_to_scrape = get_linkedin_url(21600)
                lookback = "6-hour"
            elif is_morning_run:
                url_to_scrape = get_linkedin_url(43200)
                lookback = "12-hour"
            elif is_hourly_run:
                url_to_scrape = get_linkedin_url(3600)
                lookback = "1-hour"
            else:
                url_to_scrape = LINKEDIN_URL
                lookback = "Default"
            
            console.print(f"Lookback: [cyan]{lookback}[/]")
            logging.info(f"Scraping URL: {url_to_scrape}")

            # Scrape jobs
            task_scrape = progress.add_task("[cyan]Scraping LinkedIn...", total=None)
            try:
                scraped_jobs, filename = scrape_jobs(url_to_scrape, SCRAPE_MAX_JOBS)
                stats['jobs_scraped'] = len(scraped_jobs) if scraped_jobs else 0
                progress.update(task_scrape, completed=True)
            except Exception as e:
                logging.error(f"Scraping failed: {e}", exc_info=True)
                stats['errors'].append(f"Scraping failed: {str(e)[:100]}")
                stats['status'] = 'failed'
                scraped_jobs = []
                console.print(f"[error]❌ Scraping failed: {e}[/]")
            
            if not scraped_jobs:
                console.print("[warning]No jobs found from scraper.[/]")
                stats['status'] = 'partial' if stats['errors'] else 'success'
                return

            console.print(f"✅ Scraped [bold green]{len(scraped_jobs)}[/] jobs")

            # === NEW: PRE-FILTER JOBS ===
            if FORCE_NOTIFY:
                console.print("[yellow]⚠️ FORCE_NOTIFY enabled: Skipping pre-filters[/]")
                passed_jobs = scraped_jobs
                rejected_jobs = []
                stats['pre_filter_passed'] = len(passed_jobs)
                stats['pre_filter_rejected'] = 0
            else:
                task_filter = progress.add_task("[cyan]Applying Pre-filters...", total=None)
                passed_jobs, rejected_jobs = batch_pre_filter_jobs(scraped_jobs)
                
                stats['pre_filter_passed'] = len(passed_jobs)
                stats['pre_filter_rejected'] = len(rejected_jobs)
                progress.update(task_filter, completed=True)
                
                console.print(f"🔍 Pre-filter: [green]{len(passed_jobs)} passed[/], [red]{len(rejected_jobs)} rejected[/]")
                
                # Log sample rejections for debugging
                if rejected_jobs:
                    sample_rejections = rejected_jobs[:3]
                    for job in sample_rejections:
                        logging.info(f"   ❌ Rejected: {job.get('job_title')} - {job.get('rejection_reason')}")
            
            # If no jobs passed filter, exit early
            if not passed_jobs:
                console.print("[warning]No jobs passed pre-filter criteria.[/]")
                return

            # Double-check against notifications (safety check)
            truly_new_jobs = []
            already_notified_count = 0
            
            for job in scraped_jobs:
                job_id = job.get('job_id')
                already_notified = notifications_collection.find_one({"job_id": job_id})

                if not already_notified or FORCE_NOTIFY:
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
            
            console.print(f"💾 Found [bold green]{len(truly_new_jobs)}[/] new jobs. Skipped {already_notified_count} already notified.")

            # Parse resume
            resume_data = parse_resume(RESUME_PATH)
            if not resume_data:
                console.print("[error]Failed to parse resume.[/]")
                stats['errors'].append("Resume parsing failed")
                stats['status'] = 'failed'
                stats['errors'].append("Resume parsing failed")
                stats['status'] = 'failed'
                return

            
            # === NEW: BATCH LLM MATCHING ===
            if truly_new_jobs:
                task_llm = progress.add_task(f"[cyan]Batch LLM Analysis ({len(truly_new_jobs)} jobs)...", total=None)
                
                # Call batch matcher (ONE API call for all jobs)
                match_results = batch_match_jobs(truly_new_jobs, resume_data)
                progress.update(task_llm, completed=True)
                
                if match_results:
                    stats['llm_successes'] = len(match_results)
                    stats['llm_fallbacks'] = len(truly_new_jobs) - len(match_results)
                else:
                    stats['llm_fallbacks'] = len(truly_new_jobs)
                    console.print("[warning]Batch LLM matching returned no results[/]")
            else:
                match_results = {}
                console.print("[yellow]No new jobs to analyze.[/]")
            
            # Process each job with its match result
            top_matches = []
            
            for job in truly_new_jobs:
                job_id = job.get('job_id')
                
                try:
                    # Get match data from batch results or create fallback
                    match_data = match_results.get(job_id)
                    
                    if not match_data:
                        logging.warning(f"No LLM result for {job_id}, using fallback")
                        match_data = create_fallback_match(job, "Not in batch response")
                        stats['llm_fallbacks'] += 1
                    
                    match_score = match_data.get('scores', {}).get('total', 0)
                    
                    if match_data and (match_score >= MIN_MATCH_SCORE or FORCE_NOTIFY):
                        stats['matches_found'] += 1
                        
                        top_matches.append({
                            'job_title': job.get('job_title'),
                            'company': job.get('company_name'),
                            'score': match_score
                        })
                        
                        # Save match to DB (ALL FIELDS)
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
                            "matched_at": match_data.get('matched_at')
                        }
                        matches_collection.update_one(
                            {"job_id": job_id},
                            {"$set": match_document},
                            upsert=True
                        )
                        
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
                            console.print(f"   ✅ Sent: [bold]{job.get('job_title')}[/] ({match_score:.1f}%)")
                        else:
                            console.print(f"   ⚠️ Failed: {job.get('job_title')}")
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
                            "skip_reason": f"Score {match_score} below threshold {MIN_MATCH_SCORE}",
                            "timestamp": datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)),
                            "run_type": run_type,
                            "llm_analysis": match_data.get('llm_analysis', False) if match_data else False,
                            "time_posted_hours": job.get('time_posted_hours'),
                            "applicant_count": job.get('applicant_count'),
                        }
                        notifications_collection.insert_one(notification_document)
                        # console.print(f"   📉 Skipped: {job.get('job_title')} ({match_score:.1f}%)")
                        
                except Exception as e:
                    logging.error(f"Error processing job {job_id}: {e}", exc_info=True)
                    stats['errors'].append(f"Error: {job.get('job_title', job_id)[:50]}")
                    console.print(f"[error]Error processing job {job_id}: {e}[/]")
                    
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
            
            console.print(Panel(f"Matches: {stats['matches_found']} | Sent: {stats['notifications_sent']} | Low Score: {stats['below_threshold']}", title="Results", border_style="green"))
            
            if stats['errors']:
                stats['status'] = 'partial'
            else:
                stats['status'] = 'success'
        
        # MongoDB connection automatically closed here

    except Exception as e:
        logging.error(f"Critical error: {e}", exc_info=True)
        stats['errors'].append(f"Critical error: {str(e)[:100]}")
        stats['status'] = 'failed'
        console.print(f"[bold red]CRITICAL ERROR: {e}[/]")
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
        
        logging.info("Sending summary notification...")
        send_summary_notification(stats)
        
        console.rule("[bold cyan]✅ TASK COMPLETED[/]")


def setup_scheduler():
    """Configures and starts the scheduler."""
    if not SCHEDULER_ENABLED:
        console.print("[yellow]Scheduler is disabled via environment variable.[/]")
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

    console.print("[bold green]🤖 Starting job scheduler...[/]")
    scheduler.start()
    console.print("✅ Scheduler started successfully.")
    console.print("📅 Scheduled jobs:")
    for job in scheduler.get_jobs():
        console.print(f"   • [cyan]{job.name}[/] (ID: {job.id})")
        console.print(f"     Next run: {job.next_run_time}")

    return scheduler


def create_indexes(db_instance):
    """
    Create indexes for better analytics performance.
    Run this once to optimize database queries.
    """
    logging.info("Creating database indexes for analytics...")
    
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
    console.print(Panel.fit("[bold cyan]🚀 JOB SCRAPER SCHEDULER STARTING (LLM-ENHANCED)[/]", border_style="cyan"))
    
    # Validate configuration
    validate_config()
    
    # Check if running in GitHub Actions
    if is_github_actions():
        console.print("Running in [bold]GitHub Actions[/] mode - single execution")
        # GitHub Actions will call the function directly via workflow
    else:
        # Local mode - run with scheduler
        console.print("Running in [bold]local[/] mode - with scheduler")
        
        # Run immediately on startup
        console.print("\n[bold lightning]⚡ Running IMMEDIATE startup scan...[/]")
        try:
            scrape_and_match_task(is_startup_run=True)
        except Exception as e:
            console.print(f"[bold red]Error during startup run: {e}[/]")
            logging.error(f"Error during startup run: {e}", exc_info=True)
        
        # Setup and start the scheduler
        scheduler = setup_scheduler()
        
        if scheduler:
            console.print("\n[dim]💤 Scheduler is now running in background. Press Ctrl+C to exit.[/]")
            try:
                while True:
                    time.sleep(60)
            except (KeyboardInterrupt, SystemExit):
                console.print("\n[bold red]🛑 Shutdown signal received...[/]")
                scheduler.shutdown()
                console.print("✅ Scheduler stopped gracefully.")
        else:
            console.print("No scheduler to run.")

