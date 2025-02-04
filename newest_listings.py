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
import time

# Constants
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
POST_URL = "https://maceworkflows.app.n8n.cloud/webhook/api/v1/redfin/listings"

# Apify API details
APIFY_API_TOKEN = "apify_api_jxcnLNYayfogzkZXkIpgNEVEyZINa32msyu3"
DATASET_URL = f"https://api.apify.com/v2/datasets/ktg7YLTmVlIcnodxA/items?token={APIFY_API_TOKEN}"

# Proxy Configuration
APIFY_PROXY_URL = "http://proxy.apify.com:8000"
APIFY_USERNAME = "auto"
APIFY_PASSWORD = "apify_proxy_OeAkryP7pX1EeMyH1jwfRgftdPoum739cvLc"
PROXY_GROUPS = ["SHADER", "BUYPROXIES94952"]  # Available proxy groups
MAX_RETRIES = 3

# Limit for URLs to scrape (set to 100 for testing)
URL_LIMIT = 100  

def fetch_urls_from_apify():
    """Fetch the list of URLs from the Apify dataset."""
    try:
        response = requests.get(DATASET_URL)
        if response.status_code == 200:
            # Assuming the API returns a list of URL objects with a 'url' key
            return [item['url'] for item in response.json()]
        else:
            print(f"Failed to fetch URLs from Apify. Status code: {response.status_code}, Response: {response.text}")
            return []
    except Exception as e:
        print(f"Error fetching URLs from Apify: {e}")
        return []

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

def get_proxy_url(session_id=None):
    """Generate a proxy URL with optional session ID for persistence."""
    group = PROXY_GROUPS[session_id % len(PROXY_GROUPS) if session_id is not None else 0]
    username = f"groups-{group}"
    if session_id is not None:
        username += f",session-{session_id}"
    return f"http://{username}:{APIFY_PASSWORD}@proxy.apify.com:8000"

async def fetch(session, url, index, retry_count=0):
    """Fetch page content asynchronously with proxy support and retries."""
    try:
        proxy_url = get_proxy_url(index)  # Use index as session ID for consistency
        async with session.get(url, headers=HEADERS, proxy=proxy_url) as response:
            if response.status == 403:  # Blocked by target
                if retry_count < MAX_RETRIES:
                    print(f"Blocked by target, retrying with different proxy... (Attempt {retry_count + 1})")
                    return await fetch(session, url, index, retry_count + 1)
                else:
                    print(f"Max retries reached for URL {url}")
                    return None

            html = await response.text()
            if not html.strip():
                print(f"Warning: Empty response for URL {index}: {url}")
            return html
    except Exception as e:
        if retry_count < MAX_RETRIES:
            print(f"Error fetching {url}: {e}, retrying... (Attempt {retry_count + 1})")
            return await fetch(session, url, index, retry_count + 1)
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

        # Ensure URL data is extracted correctly
        if not all([agent_name, company, phone_number, email]):
            print(f"Missing data for URL: {url}")

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

    async with ClientSession(trust_env=True) as session:
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            stats.current_batch += 1

            print(f"\nBatch {stats.current_batch}/{stats.total_batches}")
            print(f"Processing URLs: {[extract_property_address(url) for url in batch]}")

            tasks = [fetch(session, url, i + j) for j, url in enumerate(batch)]
            pages = await asyncio.gather(*tasks)

            for idx, html in enumerate(pages):
                if html:
                    result = parse_redfin_data(html, batch[idx])
                    all_results.append(result)
                else:
                    print(f"Failed to fetch HTML for URL: {batch[idx]}")

    return all_results

async def run_full_process():
    print("\nStarting Redfin Data Scraping Process\n")

    # Fetch URLs from Apify dataset
    print("\nPhase 1: Loading URLs from Apify...")
    urls = fetch_urls_from_apify()
    if not urls:
        print("No URLs found. Exiting.")
        return

    # Limit number of URLs for testing
    if len(urls) > URL_LIMIT:
        urls = urls[:URL_LIMIT]  # Limit to 100 URLs for testing

    print("\nPhase 2: Starting Scraping Process")
    print(f"Total URLs to Process: {len(urls)}")

    scraped_data = await scrape_redfin_data_async(urls)

    # Final statistics and cleanup
    print("Scraping process complete.")

def main():
    asyncio.run(run_full_process())

if __name__ == "__main__":
    main()