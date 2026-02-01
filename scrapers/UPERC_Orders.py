import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
import logging
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.uperc.org/Order_Users.aspx"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uperc_orders_downloads")

session = requests.session()
session.headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
}

def download_file(url, folder):
    try:
        # Extract filename from URL
        parsed_url = unquote(url)
        filename = os.path.basename(parsed_url)
        
        # Clean filename if needed
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

def extract_viewstate(soup):
    viewstate = soup.find("input", {"id": "__VIEWSTATE"})
    viewstate_gen = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
    event_validation = soup.find("input", {"id": "__EVENTVALIDATION"})
    
    data = {}
    if viewstate: data["__VIEWSTATE"] = viewstate["value"]
    if viewstate_gen: data["__VIEWSTATEGENERATOR"] = viewstate_gen["value"]
    if event_validation: data["__EVENTVALIDATION"] = event_validation["value"]
    
    return data

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def fetch_page(url, method="GET", data=None):
    if method == "GET":
        response = session.get(url, verify=False)
    else:
        response = session.post(url, data=data, verify=False)
    
    response.raise_for_status()
    return response.text

def scrape_and_download():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        logger.info(f"Created download directory: {DOWNLOAD_DIR}")

    # Initial GET request
    logger.info("Fetching Page 1...")
    html = fetch_page(BASE_URL)
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extract files from Page 1
    process_page(soup)
    
    # Extract ViewState for subsequent requests
    form_data = extract_viewstate(soup)
    
    # Identify total pages or loop
    # We'll just loop incrementing page number until we fail to find a link for it or some other stop condition
    page_num = 2
    max_pages = 20 # Safety limit, adjust as needed or remove for full scrape
    
    while page_num <= max_pages:
        logger.info(f"Fetching Page {page_num}...")
        
        # Prepare POST data
        # Target: ctl00$ContentPlaceHolder1$gvSchedule
        # Argument: Page${page_num}
        post_data = form_data.copy()
        post_data["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$gvSchedule"
        post_data["__EVENTARGUMENT"] = f"Page${page_num}"
        
        try:
            html = fetch_page(BASE_URL, method="POST", data=post_data)
            soup = BeautifulSoup(html, 'html.parser')
            
            # Check if we actually got the new page (sometimes it stays on same page if invalid)
            # A simple check is to see if the current page number in pagination is not a link (it's a span usually)
            # But simpler is just to process results.
            
            found_files = process_page(soup)
            if found_files == 0:
                logger.info("No files found on this page. Stopping.")
                break

            # Update ViewState for the NEXT iteration (critical!)
            form_data = extract_viewstate(soup)
            
            page_num += 1
            time.sleep(1) # Be polite
            
        except Exception as e:
            logger.error(f"Error fetching page {page_num}: {e}")
            break

def process_page(soup):
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
    logger.info(f"Processed {count} files on this page.")
    return count

if __name__ == "__main__":
    scrape_and_download()
