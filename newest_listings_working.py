import requests
import gzip
import xml.etree.ElementTree as ET
import asyncio
from aiohttp import ClientSession
import os
import json

# Constants
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
SITEMAP_MAIN_URL = "https://www.redfin.com/sitemap_com_latest_updates.xml"
JSON_FILE = "latest_updates.json"

# Apify API details
APIFY_API_TOKEN = "apify_api_jxcnLNYayfogzkZXkIpgNEVEyZINa32msyu3"
DATASET_URL = f"https://api.apify.com/v2/datasets/ktg7YLTmVlIcnodxA/items?token={APIFY_API_TOKEN}"

# Proxy Configuration
APIFY_PROXY_URL = "http://proxy.apify.com:8000"
APIFY_USERNAME = "auto"
APIFY_PASSWORD = "apify_proxy_OeAkryP7pX1EeMyH1jwfRgftdPoum739cvLc"
PROXY_GROUPS = ["SHADER", "BUYPROXIES94952"]  # Available proxy groups
MAX_RETRIES = 3

# Limit for URL payload size (in bytes)
URL_SIZE_LIMIT = 8000000  # Set to 8 MB

# Limit for URLs to scrape
URL_LIMIT = 10  # LIMIT TO FIRST 10 URLS

def get_proxy_url(session_id=None):
    """Generate a proxy URL with optional session ID for persistence."""
    group = PROXY_GROUPS[session_id % len(PROXY_GROUPS) if session_id is not None else 0]
    username = f"groups-{group}"
    if session_id is not None:
        username += f",session-{session_id}"
    return f"http://{username}:{APIFY_PASSWORD}@proxy.apify.com:8000"

async def fetch_url(url, session, session_id=0, retry_count=0):
    """Download a URL asynchronously and return its content with proxy support."""
    try:
        proxy_url = get_proxy_url(session_id)
        async with session.get(url, headers=HEADERS, proxy=proxy_url) as response:
            if response.status == 403:  # Blocked by target
                if retry_count < MAX_RETRIES:
                    print(f"Blocked by target, retrying with different proxy... (Attempt {retry_count + 1})")
                    return await fetch_url(url, session, session_id + 1, retry_count + 1)
                else:
                    print(f"Max retries reached for URL {url}")
                    return None

            if response.status == 200:
                content = await response.read()
                return content.decode("utf-8")
            else:
                if retry_count < MAX_RETRIES:
                    print(f"Failed to fetch {url}: {response.status}, retrying...")
                    return await fetch_url(url, session, session_id + 1, retry_count + 1)
                print(f"Failed to fetch {url}: {response.status} after {MAX_RETRIES} retries")
                return None
    except Exception as e:
        if retry_count < MAX_RETRIES:
            print(f"Error fetching {url}: {e}, retrying... (Attempt {retry_count + 1})")
            return await fetch_url(url, session, session_id + 1, retry_count + 1)
        print(f"Max retries reached for URL {url}: {e}")
        return None

def parse_sitemap(xml_content):
    """Parse the XML content and extract all URLs."""
    try:
        root = ET.fromstring(xml_content)
        urls = [elem.text for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
        return urls
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return []

async def save_to_apify(urls):
    """Save extracted URLs to Apify dataset using the POST API."""
    try:
        # Format the URLs as an array of objects
        formatted_urls = [{"url": url} for url in urls]
        response = requests.post(DATASET_URL, json=formatted_urls)
        if response.status_code == 201:
            print(f"Successfully saved {len(urls)} URLs to Apify storage")
        else:
            print(f"Failed to save URLs to Apify. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error saving to Apify: {e}")

async def process_sitemap(url, session, session_id, semaphore):
    """Process a single sitemap URL with semaphore control."""
    async with semaphore:
        print(f"Processing sitemap: {url}")
        content = await fetch_url(url, session, session_id=session_id)
        if content:
            return parse_sitemap(content)
        return []

async def main():
    # Configure concurrency control
    MAX_CONCURRENT_REQUESTS = 7
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # Create a single session for all requests
    async with ClientSession() as session:
        # Step 1: Fetch the main sitemap and extract URLs
        print("Fetching main sitemap...")
        main_sitemap_content = await fetch_url(SITEMAP_MAIN_URL, session)
        if not main_sitemap_content:
            print("Failed to retrieve main sitemap.")
            return

        update_urls = parse_sitemap(main_sitemap_content)
        if not update_urls:
            print("No URLs found in the main sitemap.")
            return

        # Limit URLs to the first URL_LIMIT entries
        limited_urls = update_urls[:URL_LIMIT]

        collected_urls = []
        total_size = 0

        # Step 2: Process sitemaps concurrently in batches
        print(f"Processing {len(limited_urls)} sitemaps concurrently (max {MAX_CONCURRENT_REQUESTS} at a time)...")
        tasks = [
            process_sitemap(url, session, i, semaphore)
            for i, url in enumerate(limited_urls)
        ]

        # Gather results from all tasks
        results = await asyncio.gather(*tasks)

        for sublist in results:
            if sublist:
                for url in sublist:
                    encoded_url = url.encode('utf-8')  # Encode to get byte size
                    url_size = len(encoded_url)

                    # Check if adding this URL exceeds the limit
                    if total_size + url_size > URL_SIZE_LIMIT:
                        # Save the current collected URLs before adding new ones
                        if collected_urls:  # Save only if there are URLs to save
                            await save_to_apify(collected_urls)
                        collected_urls.clear()  # Clear the list for the next batch
                        total_size = 0  # Reset size counter

                    # Collect the URL
                    collected_urls.append(url)
                    total_size += url_size  # Update the total size

        # Final check to save any remaining URLs
        if collected_urls:
            await save_to_apify(collected_urls)

if __name__ == "__main__":
    asyncio.run(main())