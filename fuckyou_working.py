import requests
from bs4 import BeautifulSoup
import asyncio
from aiohttp import ClientSession
import json
import os
import uuid
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from newest_listings import main as fetch_listings
import time

# Constants
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
POST_URL = "https://maceworkflows.app.n8n.cloud/webhook/api/v1/redfin/listings"
JSON_FILE = "latest_updates.json"

# Proxy Configuration
APIFY_PROXY_URL = "http://proxy.apify.com:8000"
APIFY_USERNAME = "auto"
APIFY_PASSWORD = "apify_proxy_OeAkryP7pX1EeMyH1jwfRgftdPoum739cvLc"
PROXY_GROUPS = ["SHADER", "BUYPROXIES94952"]  # Available proxy groups
MAX_RETRIES = 3


def load_urls_from_json():
    """Loads property URLs from the latest_updates.json file."""
    if not os.path.exists(JSON_FILE):
        print(f"Error: {JSON_FILE} not found. Please run newest_listings.py first.")
        return []
    
    with open(JSON_FILE, "r", encoding="utf-8") as file:
        urls = json.load(file)
    
    print(f"Loaded {len(urls)} property URLs from JSON file.")
    return urls


def extract_property_address(url):
    """Extracts the full property address from the given Redfin URL."""
    parts = url.split("/")
    if len(parts) > 5:
        return parts[5]  # Extracts full address from URL
    return "Unknown Address"


def extract_location_from_url(url):
    """Extracts the state, city, and zip from the Redfin URL."""
    parts = url.split("/")
    if len(parts) > 6:
        state = parts[3]
        city = parts[4].replace("-", " ")
        zip_code = parts[5].split("-")[-1]  # Extracts only the last segment of the zip part
        return state, city, zip_code
    return "N/A", "N/A", "N/A"
    return "N/A", "N/A", "N/A"


def get_proxy_url(session_id=None):
    """Generate a proxy URL with optional session ID for persistence."""
    group = PROXY_GROUPS[session_id % len(PROXY_GROUPS) if session_id is not None else 0]
    username = f"groups-{group}"
    if session_id is not None:
        username += f",session-{session_id}"
    return f"http://{username}:{APIFY_PASSWORD}@proxy.apify.com:8000"

async def fetch(session, url, index, retry_count=0, stats=None):
    """Fetch page content asynchronously with proxy support and retries."""
    try:
        proxy_url = get_proxy_url(index)  # Use index as session ID for consistency
        async with session.get(url, headers=HEADERS, proxy=proxy_url) as response:
            if response.status == 403:  # Blocked by target
                if retry_count < MAX_RETRIES:
                    if stats:
                        stats.update(retry=True, proxy_switch=True)
                    print(f"Blocked by target, retrying with different proxy... (Attempt {retry_count + 1})")
                    return await fetch(session, url, index, retry_count + 1, stats)
                else:
                    print(f"Max retries reached for URL {url}")
                    return None
                    
            html = await response.text()
            if not html.strip():
                print(f"Warning: Empty response for URL {index}: {url}")
            return html
    except Exception as e:
        if retry_count < MAX_RETRIES:
            if stats:
                stats.update(retry=True)
            print(f"Error fetching {url}: {e}, retrying... (Attempt {retry_count + 1})")
            return await fetch(session, url, index, retry_count + 1, stats)
        print(f"Max retries reached for URL {url}: {e}")
        return None


def parse_redfin_data(html, url):
    """Parse HTML to extract listing agent details and location details from URL."""
    try:
        soup = BeautifulSoup(html, "html.parser")

        agent_name = soup.select_one(".agent-basic-details--heading span, .font-weight-bold")
        company = soup.select_one(".agent-basic-details--broker span, .UnservicedHomesVariant__primaryBody div:nth-child(3)")
        phone_number = soup.select_one(".agent-extra-info--phone div span a, .contactPhoneNumber")
        email = soup.select_one(".agent-extra-info--email div span a, .contactEmail")

        state, city, zip_code = extract_location_from_url(url)

        return {
            "Property Address": extract_property_address(url),
            "Agent Name": agent_name.get_text(strip=True) if agent_name else "N/A",
            "Company": company.get_text(strip=True) if company else "N/A",
            "Phone Number": phone_number.get_text(strip=True) if phone_number else "N/A",
            "Email": email.get("href").replace("mailto:", "") if email else "N/A",
            "City": city,
            "State": state,
            "Zip": zip_code,
            "URL": url
        }
    except Exception as e:
        print(f"Error parsing HTML for {url}: {e}")
        return {
            "Property Address": extract_property_address(url),
            "Agent Name": "N/A",
            "Company": "N/A",
            "Phone Number": "N/A",
            "Email": "N/A",
            "City": "N/A",
            "State": "N/A",
            "Zip": "N/A",
            "URL": url
        }


class ScrapingStats:
    def __init__(self, total_urls):
        self.total_urls = total_urls
        self.successful_scrapes = 0
        self.failed_scrapes = 0
        self.start_time = time.time()
        self.current_batch = 0
        self.total_batches = 0
        self.api_submissions = 0
        self.proxy_switches = 0
        self.retry_attempts = 0

    def update(self, success=True, retry=False, proxy_switch=False):
        if success:
            self.successful_scrapes += 1
        else:
            self.failed_scrapes += 1
        if retry:
            self.retry_attempts += 1
        if proxy_switch:
            self.proxy_switches += 1

    def get_elapsed_time(self):
        elapsed = time.time() - self.start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

async def scrape_redfin_data_async(urls, batch_size=100):
    """Scrape listing agent details asynchronously in smaller batches with proxy support."""
    all_results = []
    stats = ScrapingStats(len(urls))
    stats.total_batches = (len(urls) + batch_size - 1) // batch_size
    
    # Configure ClientSession with proxy settings
    conn = ClientSession(trust_env=True)
    
    # Create progress bar for overall progress
    with tqdm(total=len(urls), desc="Overall Progress", unit="listings") as pbar:
        async with conn as session:
            for i in range(0, len(urls), batch_size):
                batch = urls[i:i+batch_size]
                stats.current_batch += 1
                batch_start = time.time()
                
                print(f"\n{'='*80}")
                print(f"Batch {stats.current_batch}/{stats.total_batches}")
                print(f"Time Elapsed: {stats.get_elapsed_time()}")
                print(f"Success Rate: {(stats.successful_scrapes/stats.total_urls*100):.2f}%")
                print(f"Failed Attempts: {stats.failed_scrapes}")
                print(f"Proxy Switches: {stats.proxy_switches}")
                print(f"Retry Attempts: {stats.retry_attempts}")
                print(f"{'='*80}\n")
                
                tasks = [fetch(session, url, i + j, stats=stats) for j, url in enumerate(batch)]
                pages = await asyncio.gather(*tasks)
                
                # Process results with batch progress bar
                batch_results = []
                with tqdm(total=len(batch), desc="Current Batch", unit="listing", leave=False) as batch_pbar:
                    for idx, html in enumerate(pages):
                        if html:
                            result = parse_redfin_data(html, batch[idx])
                            batch_results.append(result)
                            stats.update(success=True)
                        else:
                            stats.update(success=False)
                        batch_pbar.update(1)
                        pbar.update(1)
                
                all_results.extend(batch_results)
                successful_scrapes = len(batch_results)
                
                # If we've reached 1000 successful scrapes, send the data
                if successful_scrapes >= 1000:
                    print(f"\nSending batch of {successful_scrapes} listings to API...")
                    post_data_to_api(all_results[-successful_scrapes:])
                    stats.api_submissions += 1
                    print(f"API submission {stats.api_submissions} complete")
                
                # Calculate and display batch statistics
                batch_time = time.time() - batch_start
                urls_per_second = len(batch) / batch_time
                print(f"\nBatch Statistics:")
                print(f"Processing Speed: {urls_per_second:.2f} URLs/second")
                print(f"Batch Success Rate: {(len(batch_results)/len(batch)*100):.2f}%")
                
                # Small delay between batches to avoid overwhelming the proxy
                if i + batch_size < len(urls):
                    await asyncio.sleep(1)
    
    # Send any remaining results
    if successful_scrapes > 0:
        print(f"Sending final batch of {successful_scrapes} scraped listings to API...")
        post_data_to_api(all_results[-successful_scrapes:])
    
    return all_results


def post_data_to_api(data):
    """Sends scraped data as a POST request to the API."""
    batch_id = str(uuid.uuid4())
    date_created = datetime.now().isoformat()
    payload = {
        "batchId": batch_id,
        "totalListings": len(data),
        "listings": [
            {
                "url": entry["URL"],
                "agentName": entry["Agent Name"],
                "agentEmail": entry["Email"],
                "agentPhoneNumber": entry["Phone Number"],
                "additionalInfo": "Scraped from Redfin",
                "city": entry["City"],
                "state": entry["State"],
                "zip": entry["Zip"]
            }
            for entry in data
        ],
        "dateCreated": date_created
    }
    
    response = requests.post(POST_URL, json=payload)
    print(f"Status Code: {response.status_code}")
    print(f"Response Body: {response.text}")


async def run_full_process():
    print("\n" + "="*80)
    print("Starting Redfin Data Scraping Process")
    print("="*80 + "\n")
    
    start_time = time.time()
    
    # First run the newest_listings script
    print("Phase 1: Fetching latest listings...")
    await fetch_listings()
    
    # Check if JSON file exists and has content
    print("\nPhase 2: Loading URLs from JSON...")
    urls = load_urls_from_json()
    if not urls:
        print("No URLs found. Exiting.")
        return
    
    print("\nPhase 3: Starting Scraping Process")
    print(f"Total URLs to Process: {len(urls)}")
    print(f"Estimated Time: {len(urls)/100:.1f} minutes (at ~100 URLs/minute)\n")
    
    scraped_data = await scrape_redfin_data_async(urls)
    
    # Final Statistics
    total_time = time.time() - start_time
    print("\n" + "="*80)
    print("Scraping Process Complete")
    print(f"Total Time: {total_time/60:.2f} minutes")
    print(f"Total URLs Processed: {len(urls)}")
    print(f"Successfully Scraped: {len(scraped_data)}")
    print(f"Success Rate: {(len(scraped_data)/len(urls)*100):.2f}%")
    print("="*80 + "\n")

def main():
    asyncio.run(run_full_process())

if __name__ == "__main__":
    main()
