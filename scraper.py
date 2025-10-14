# scraper.py (updated to use enhanced scraper)
import random
import time
import json
import os
import hashlib
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Import the enhanced scraper
from llm_scraper import scrape_job_details_enhanced, scrape_job_details_basic


def random_delay(min_sec, max_sec):
    """Pauses execution for a random duration between min_sec and max_sec seconds."""
    delay = random.uniform(min_sec, max_sec)
    print(f"⏳ Waiting {delay:.2f} seconds...")
    time.sleep(delay)


def human_scroll(page):
    """Scrolls the page naturally, mimicking human behavior."""
    print("📜 Scrolling page naturally...")
    scroll_count = 0
    for _ in range(random.randint(5, 10)):
        scroll_increment = random.randint(200, 500)
        page.evaluate(f"window.scrollBy(0, {scroll_increment})")
        random_delay(0.5, 1)
        scroll_count += 1

        # 10% chance to scroll back up a bit
        if random.random() < 0.1:
            scroll_increment = random.randint(100, 300)
            page.evaluate(f"window.scrollBy(0, -{scroll_increment})")
            random_delay(0.5, 1)
    print(f"✅ Scrolled {scroll_count} times")


def extract_job_urls(page):
    """Extracts job URLs from the search results page."""
    print("📋 Extracting job URLs...")
    job_urls = []
    job_cards = page.query_selector_all(".jobs-search__results-list li")
    print(f"📊 Found {len(job_cards)} potential job cards")
    for card in job_cards:
        job_url_element = card.query_selector("a.base-card__full-link")
        if job_url_element:
            job_url = job_url_element.get_attribute("href")
            if job_url:
                job_urls.append(job_url)
    print(f"✅ Extracted {len(job_urls)} job URLs.")
    return job_urls


# scraper.py (UPDATED - Filter duplicates BEFORE reporting count)

def scrape_jobs(url: str, max_jobs: int = 30):
    """Main function to scrape LinkedIn jobs with early deduplication."""
    from database import MongoDB

    is_ci = os.getenv('GITHUB_ACTIONS') == 'true' or os.getenv('CI') == 'true'
    
    print("🚀 Starting LinkedIn Scraper (LLM-Enhanced)")
    
    # Connect to DB early for deduplication check
    db_instance = MongoDB()
    jobs_collection = db_instance.get_collection("jobs")
    notifications_collection = db_instance.get_collection("notifications")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=is_ci,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )
        print("🌐 Launching browser...")

        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        """)

        print("✅ Browser launched")
        print(f"🔗 Navigating to: {url}")
        page.goto(url)
        random_delay(0.5, 3)
        print("✅ Page loaded")

        human_scroll(page)

        job_urls = extract_job_urls(page)
        job_urls = job_urls[:max_jobs]

        print(f"\n📝 Found {len(job_urls)} job URLs, now checking which are NEW...")
        
        # PRE-FILTER: Check which jobs are already processed
        new_job_urls = []
        already_seen_count = 0
        
        for job_url in job_urls:
            # Generate job_id from clean URL
            from llm_scraper import extract_linkedin_job_id_from_url
            job_id = extract_linkedin_job_id_from_url(job_url)
            
            # Check if already notified (notifications = source of truth)
            already_notified = notifications_collection.find_one({"job_id": job_id})
            
            if not already_notified:
                new_job_urls.append(job_url)
            else:
                already_seen_count += 1
        
        print(f"✅ Deduplication complete:")
        print(f"   • New jobs to scrape: {len(new_job_urls)}")
        print(f"   • Already processed: {already_seen_count}")
        
        # Only scrape NEW jobs
        if not new_job_urls:
            print("⚠️  No new jobs to scrape. All jobs already processed.")
            browser.close()
            db_instance.close_connection()
            return [], None

        print(f"\n📝 Scraping details for {len(new_job_urls)} NEW jobs...")
        scraped_jobs = []
        
        for i, job_url in enumerate(new_job_urls):
            print(f"\n[{i+1}/{len(new_job_urls)}] Processing: {job_url}")
            
            try:
                job_details = scrape_job_details_enhanced(browser, job_url)
            except Exception as e:
                print(f"  ⚠️  Enhanced scraper failed: {e}")
                print(f"  🔄 Falling back to basic scraper")
                try:
                    job_details = scrape_job_details_basic(browser, job_url)
                except Exception as e2:
                    print(f"  ❌ Basic scraper also failed: {e2}")
                    job_details = None
            
            if job_details:
                scraped_jobs.append(job_details)
            
            random_delay(0.5, 1)

        browser.close()
        db_instance.close_connection()
        
        print(f"\n✅ Scraping Complete: {len(scraped_jobs)} NEW jobs scraped (skipped {already_seen_count} already seen)")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"scraped_data/linkedin_jobs_{timestamp}.json"
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(scraped_jobs, f, ensure_ascii=False, indent=4)
        print(f"💾 Saved job data to {filename}")

        return scraped_jobs, filename
