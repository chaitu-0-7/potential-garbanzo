# discord_notifier.py (UPDATED - Enhanced with job metadata)
import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import pytz

load_dotenv()

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata")


def get_color_for_score(score):
    """Returns Discord color based on match score."""
    if score >= 80:
        return 0x2ecc71  # Green - Excellent match
    elif score >= 70:
        return 0x3498db  # Blue - Good match
    elif score >= 60:
        return 0xf39c12  # Orange - Decent match
    else:
        return 0xe74c3c  # Red - Poor match


def get_recommendation_emoji(recommendation):
    """Get emoji for recommendation."""
    emoji_map = {
        "APPLY_IMMEDIATELY": "🚀",
        "APPLY": "✅",
        "CONSIDER": "🤔",
        "SKIP": "⏭️"
    }
    return emoji_map.get(recommendation, "📋")


def format_time_ago(posted_at_iso: str) -> str:
    """
    Format timestamp to human-readable time ago.
    E.g., "2 hours ago", "1 day ago"
    """
    if not posted_at_iso:
        return "Unknown"
    
    try:
        posted_at = datetime.fromisoformat(posted_at_iso)
        current_time = datetime.now(pytz.timezone(SCHEDULER_TIMEZONE))
        
        # Make both timezone-aware if needed
        if posted_at.tzinfo is None:
            posted_at = pytz.timezone(SCHEDULER_TIMEZONE).localize(posted_at)
        if current_time.tzinfo is None:
            current_time = pytz.timezone(SCHEDULER_TIMEZONE).localize(current_time)
        
        diff = current_time - posted_at
        
        hours = diff.total_seconds() / 3600
        
        if hours < 1:
            minutes = int(diff.total_seconds() / 60)
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif hours < 24:
            hours = int(hours)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif hours < 168:  # Less than a week
            days = int(hours / 24)
            return f"{days} day{'s' if days != 1 else ''} ago"
        else:
            weeks = int(hours / 168)
            return f"{weeks} week{'s' if weeks != 1 else ''} ago"
    except Exception as e:
        logging.error(f"Error formatting time ago: {e}")
        return "Unknown"


def format_applicant_count(count: int) -> str:
    """Format applicant count with appropriate emoji and text."""
    if count is None:
        return "Unknown"
    
    if count < 25:
        return f"🎯 {count} applicants (Low competition!)"
    elif count < 100:
        return f"👥 {count} applicants (Moderate)"
    elif count < 200:
        return f"📊 {count} applicants (High)"
    else:
        return f"🔥 {count}+ applicants (Very competitive)"


def truncate_text(text, max_length):
    """Safely truncate text to fit Discord limits."""
    if not text:
        return "N/A"
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def send_discord_notification(job_match: dict):
    """Send enhanced Discord notification with job metadata."""
    print(f"DEBUG: job_match in send_discord_notification: {job_match}")
    
    if not DISCORD_WEBHOOK_URL:
        print("❌ DISCORD_WEBHOOK_URL not found in .env file.")
        return "error_no_webhook"

    job = job_match.get("job", {})
    match = job_match.get("match", {})

    # Extract job details
    job_id = job.get("job_id", "N/A")
    job_title = job.get("job_title", "N/A")
    company_name = job.get("company_name", "N/A")
    location = job.get("location", "N/A")
    job_url = job.get("url", "#")
    
    # Extract job metadata
    posted_at = job.get("posted_at")
    time_ago = format_time_ago(posted_at)
    applicant_count = job.get("applicant_count")
    applicant_text = format_applicant_count(applicant_count)
    employment_type = job.get("employment_type", "Not specified")
    workplace_type = job.get("workplace_type", "Not specified")
    seniority_level = job.get("seniority_level", "Not specified")
    
    # Extract match details
    match_score = match.get('scores', {}).get('total', 0) if match else 0
    classification = match.get('classification', "N/A") if match else "N/A"
    matched_skills = match.get('matched_skills', []) if match else []
    missing_skills = match.get('skill_gaps', []) if match else []
    
    # LLM-specific fields
    is_llm_analysis = match.get('llm_analysis', False)
    recommendation = match.get('recommendation', 'CONSIDER')
    reasoning = match.get('reasoning', '')
    strengths = match.get('strengths', [])
    weaknesses = match.get('weaknesses', [])
    interview_tips = match.get('interview_tips', [])
    parsed_details = match.get('parsed_job_details', {})
    min_experience = parsed_details.get('min_experience_years')
    if min_experience is None:
        min_experience = parsed_details.get('required_experience_years')
    
    # Get individual scores
    technical_score = match.get('scores', {}).get('technical', 0) if match else 0
    experience_score = match.get('scores', {}).get('experience', 0) if match else 0
    domain_score = match.get('scores', {}).get('domain', 0) if match else 0

    # Truncate description
    job_description = truncate_text(job.get("description", "N/A"), 250)
    
    # Format matched skills
    skills_text = ", ".join(matched_skills[:10]) if matched_skills else "N/A"
    if len(matched_skills) > 10:
        skills_text += f" (+{len(matched_skills) - 10} more)"
    
    # Format missing skills
    missing_text = ", ".join(missing_skills[:5]) if missing_skills else "None"
    if len(missing_skills) > 5:
        missing_text += f" (+{len(missing_skills) - 5} more)"
    
    # Build description with job metadata
    description_parts = [f"**{company_name}** • {location}"]
    description_parts.append(f"⏰ Posted {time_ago}")
    description_parts.append(applicant_text)
    
    if is_llm_analysis:
        description_parts.append("🤖 AI-Analyzed")
    
    # Build fields list
    fields = [
        {
            "name": "📊 Match Score",
            "value": f"**{match_score:.1f}%** ({classification})",
            "inline": True
        },
        {
            "name": "🔧 Technical",
            "value": f"{technical_score:.1f}%",
            "inline": True
        },
        {
            "name": "💼 Experience",
            "value": f"{experience_score:.1f}%",
            "inline": True
        }
    ]
    
    # Add job details section
    fields.append({
        "name": "💼 Job Details",
        "value": f"**Type:** {employment_type}\n**Work:** {workplace_type}\n**Level:** {seniority_level}",
        "inline": False
    })

    # Add Experience Required field if available
    if min_experience is not None:
        fields.append({
            "name": "🎓 Experience Required",
            "value": f"**{min_experience} years**",
            "inline": True
        })
    else:
        # Show that we couldn't extract this info
        fields.append({
            "name": "🎓 Experience Required",
            "value": "⚠️ *Not found - review JD*",
            "inline": True
        })
    
    # Add LLM-specific insights if available
    if is_llm_analysis:
        # Add recommendation
        fields.append({
            "name": f"{get_recommendation_emoji(recommendation)} Recommendation",
            "value": recommendation.replace('_', ' ').title(),
            "inline": True
        })
        
        # Add applicant count as separate field for emphasis
        if applicant_count:
            competition_level = "Low 🎯" if applicant_count < 50 else ("Medium 📊" if applicant_count < 150 else "High 🔥")
            fields.append({
                "name": "👥 Competition",
                "value": competition_level,
                "inline": True
            })
        
        # Add blank for alignment
        fields.append({"name": "\u200b", "value": "\u200b", "inline": True})
        
        # Add strengths
        if strengths:
            strengths_text = "\n".join([f"✅ {s}" for s in strengths[:3]])
            fields.append({
                "name": "💪 Your Strengths",
                "value": truncate_text(strengths_text, 1024),
                "inline": False
            })
        
        # Add LLM reasoning
        if reasoning:
            fields.append({
                "name": "🤖 AI Analysis",
                "value": truncate_text(reasoning, 1024),
                "inline": False
            })
        
        # Add interview tips
        if interview_tips:
            tips_text = "\n".join([f"💡 {tip}" for tip in interview_tips[:3]])
            fields.append({
                "name": "🎯 Interview Tips",
                "value": truncate_text(tips_text, 1024),
                "inline": False
            })
    
    # Add matched skills
    fields.append({
        "name": "🎯 Matched Skills",
        "value": truncate_text(skills_text, 1024),
        "inline": False
    })
    
    # Add missing skills if any (and not too many)
    if missing_skills and len(missing_skills) <= 5:
        fields.append({
            "name": "📚 Skills to Develop",
            "value": truncate_text(missing_text, 512),
            "inline": False
        })
    
    # Add description preview
    fields.append({
        "name": "📝 Description Preview",
        "value": job_description,
        "inline": False
    })
    
    # Add quick actions (REMOVED BROKEN LINK)
    fields.append({
        "name": "🔗 Quick Actions",
        "value": f"[View Full Job]({job_url})",
        "inline": False
    })
    
    # Construct embed
    embed = {
        "title": truncate_text(f"🎯 {job_title}", 256),
        "url": job_url,
        "color": get_color_for_score(match_score),
        "description": "\n".join(description_parts),
        "fields": fields,
        "footer": {
            "text": f"Job ID: {job_id}"
        },
        "timestamp": datetime.utcnow().isoformat()
    }

    payload = {
        "embeds": [embed]
    }

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        print(f"✅ Discord notification sent successfully. Status Code: {response.status_code}")
        return "success"
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to send Discord notification: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print("Response content:", e.response.text)
        return "error_send_failed"
# discord_notifier.py (ADD THIS FUNCTION AT THE END)

def send_summary_notification(summary_data: dict):
    """
    Send execution summary to Discord with pre-filter statistics.
    
    Args:
        summary_data: Dict containing run statistics
    """
    if not DISCORD_WEBHOOK_URL:
        print("❌ DISCORD_WEBHOOK_URL not found.")
        return "error_no_webhook"
    
    run_type = summary_data.get('run_type', 'Unknown')
    status = summary_data.get('status', 'success')
    
    # Determine color based on status
    if status == 'success':
        color = 0x2ecc71  # Green
        emoji = "✅"
        title = f"{emoji} Job Scraper Completed Successfully"
    elif status == 'partial':
        color = 0xf39c12  # Orange
        emoji = "⚠️"
        title = f"{emoji} Job Scraper Completed with Warnings"
    else:
        color = 0xe74c3c  # Red
        emoji = "❌"
        title = f"{emoji} Job Scraper Failed"
    
    # Build description
    timestamp = summary_data.get('timestamp', datetime.now(pytz.timezone(SCHEDULER_TIMEZONE)))
    description = f"**Run Type:** {run_type.replace('_', ' ').title()}\n"
    description += f"**Time:** {timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    
    # Build fields
    fields = []
    
    # Scraping statistics
    jobs_found = summary_data.get('jobs_found_on_linkedin', 0)
    scraped_count = summary_data.get('jobs_scraped', 0)
    already_seen = summary_data.get('already_seen', 0)
    already_notified = summary_data.get('already_notified', 0)
    new_jobs_count = summary_data.get('new_jobs', 0)
    
    scraping_text = f"**Found on LinkedIn:** {jobs_found}\n" if jobs_found > 0 else ""
    scraping_text += f"**New Jobs Scraped:** {scraped_count}\n"
    scraping_text += f"**Already Seen:** {already_seen}\n" if already_seen > 0 else ""
    scraping_text += f"**Already Notified:** {already_notified}\n" if already_notified > 0 else ""
    scraping_text += f"**Ready to Match:** {new_jobs_count}"
    
    fields.append({
        "name": "📥 Scraping Results",
        "value": scraping_text,
        "inline": True
    })
    
    # === NEW: Pre-Filter Statistics ===
    pre_filter_passed = summary_data.get('pre_filter_passed', 0)
    pre_filter_rejected = summary_data.get('pre_filter_rejected', 0)
    api_calls_saved = pre_filter_rejected
    
    if scraped_count > 0:  # Only show if we actually scraped jobs
        filter_pass_rate = (pre_filter_passed / scraped_count * 100) if scraped_count > 0 else 0
        
        filter_text = f"**Passed Filter:** {pre_filter_passed}\n"
        filter_text += f"**Rejected:** {pre_filter_rejected}\n"
        filter_text += f"**Pass Rate:** {filter_pass_rate:.1f}%\n"
        filter_text += f"**💰 API Calls Saved:** {api_calls_saved}"
        
        fields.append({
            "name": "🔍 Pre-Filter Results",
            "value": filter_text,
            "inline": True
        })
    
    # Add blank field for alignment
    fields.append({"name": "\u200b", "value": "\u200b", "inline": True})
    
    # Matching statistics
    matches_found = summary_data.get('matches_found', 0)
    notifications_sent = summary_data.get('notifications_sent', 0)
    below_threshold = summary_data.get('below_threshold', 0)
    
    matching_text = f"**High Matches:** {matches_found}\n"
    matching_text += f"**Notifications Sent:** {notifications_sent}\n"
    matching_text += f"**Below Threshold:** {below_threshold}"
    
    fields.append({
        "name": "🎯 Matching Results",
        "value": matching_text,
        "inline": True
    })
    
    # LLM usage statistics (if available)
    llm_successes = summary_data.get('llm_successes', 0)
    llm_fallbacks = summary_data.get('llm_fallbacks', 0)
    total_processed = llm_successes + llm_fallbacks
    
    if total_processed > 0:
        llm_success_rate = (llm_successes / total_processed) * 100
        llm_text = f"**LLM Analysis:** {llm_successes}/{total_processed} ({llm_success_rate:.1f}%)\n"
        llm_text += f"**Fallback Used:** {llm_fallbacks}"
        
        fields.append({
            "name": "🤖 AI Performance",
            "value": llm_text,
            "inline": True
        })
    
    # Execution time
    execution_time = summary_data.get('execution_time_seconds', 0)
    fields.append({
        "name": "⏱️ Execution Time",
        "value": f"{execution_time:.1f} seconds",
        "inline": True
    })
    
    # Error information (if any)
    errors = summary_data.get('errors', [])
    if errors:
        error_text = "\n".join([f"• {err}" for err in errors[:3]])
        if len(errors) > 3:
            error_text += f"\n... and {len(errors) - 3} more"
        
        fields.append({
            "name": "⚠️ Errors/Warnings",
            "value": error_text,
            "inline": False
        })
    
    # Top matches (if any)
    top_matches = summary_data.get('top_matches', [])
    if top_matches:
        matches_text = ""
        for match in top_matches[:3]:
            matches_text += f"• **{match.get('job_title', 'Unknown')}** at {match.get('company', 'Unknown')} "
            matches_text += f"({match.get('score', 0):.0f}%)\n"
        
        fields.append({
            "name": "🏆 Top Matches This Run",
            "value": matches_text.strip(),
            "inline": False
        })
    
    # Next run information (for scheduled runs)
    next_run = summary_data.get('next_run_time')
    if next_run:
        fields.append({
            "name": "⏭️ Next Run",
            "value": next_run,
            "inline": False
        })
    
    # Construct embed
    embed = {
        "title": title,
        "color": color,
        "description": description,
        "fields": fields,
        "footer": {
            "text": "Job Scraper Automation"
        },
        "timestamp": datetime.utcnow().isoformat()
    }
    
    payload = {
        "embeds": [embed]
    }
    
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        print(f"✅ Summary notification sent successfully.")
        return "success"
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to send summary notification: {e}")
        return "error_send_failed"
