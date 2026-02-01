import logging
import os
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# BASE_URL = "https://www.uperc.org/Notified_User.aspx"
BASE_URL  = "https://www.uperc.org/Order_Users.aspx"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uperc_downloads")

session = requests.session()
session.headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
}

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def fetch_page(url):
    response = session.get(url, verify=False) # verify=False as per other scrapers often needing it for gov sites
    response.raise_for_status()
    return response.text

def download_file(url, folder):
    try:
        # Extract filename from URL
        parsed_url = unquote(url)
        filename = os.path.basename(parsed_url)
        
        # Clean filename if needed (sometimes query params are attached)
        if '?' in filename:
            filename = filename.split('?')[0]
            
        filepath = os.path.join(folder, filename)
        
        if os.path.exists(filepath):
            logger.info(f"File already exists: {filename}")
            return

        logger.info(f"Downloading: {filename}")
        with session.get(url, stream=True, verify=False) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        logger.info(f"Saved to: {filepath}")
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")

def scrape_and_download():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        logger.info(f"Created download directory: {DOWNLOAD_DIR}")

    try:
        logger.info(f"Fetching {BASE_URL}...")
        html = fetch_page(BASE_URL)
        soup = BeautifulSoup(html, 'html.parser')

        # Find all links
        links = soup.find_all('a', href=True)
        
        count = 0
        for link in links:
            href = link['href']
            # Check for file extensions
            lower_href = href.lower()
            if lower_href.endswith(('.pdf', '.rar', '.zip')):
                full_url = urljoin(BASE_URL, href)
                download_file(full_url, DOWNLOAD_DIR)
                count += 1
        
        logger.info(f"Download complete. Processed {count} files.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    scrape_and_download()
