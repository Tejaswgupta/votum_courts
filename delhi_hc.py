import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

try:
    from .order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage
except ImportError:
    from order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)

BASE_URL = "https://delhihighcourt.nic.in"
SEARCH_URL = f"{BASE_URL}/app/get-case-type-status"
VALIDATE_CAPTCHA_URL = f"{BASE_URL}/app/validateCaptcha"

class DelhiHCService:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            # 'X-Requested-With': 'XMLHttpRequest'  <-- Removed from global headers
        }
        self.csrf_token = None

    def _get_initial_state(self):
        """
        Fetch the main page to get cookies, CSRF token, and the initial CAPTCHA code.
        """
        try:
            # Standard page load, accept HTML
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
            }
            resp = self.session.get(SEARCH_URL, headers=headers, timeout=30)
            resp.raise_for_status()
            
            # Extract CSRF Token
            # Pattern: data: { "_token": "TOKEN", ... }
            token_match = re.search(r'data:\s*\{\s*"_token":\s*"([^"]+)"', resp.text)
            if token_match:
                self.csrf_token = token_match.group(1)
            else:
                logger.warning("Could not find CSRF token in page source.")

            # Extract CAPTCHA code
            # Pattern: <span id="captcha-code" class="captcha-code">CODE</span>
            soup = BeautifulSoup(resp.text, 'html.parser')
            captcha_span = soup.find('span', {'id': 'captcha-code'})
            captcha_code = captcha_span.text.strip() if captcha_span else None
            
            # Fallback to hidden input if span is empty (unlikely given source)
            if not captcha_code:
                random_id_input = soup.find('input', {'id': 'randomid'})
                if random_id_input:
                    captcha_code = random_id_input.get('value')
            
            return captcha_code

        except Exception as e:
            logger.error(f"Error fetching initial state: {e}")
            return None

    def validate_captcha(self, captcha_code: str) -> bool:
        """
        Validate the CAPTCHA with the server.
        """
        if not self.csrf_token or not captcha_code:
            return False
            
        try:
            data = {
                "_token": self.csrf_token,
                "captchaInput": captcha_code
            }
            headers = {'X-Requested-With': 'XMLHttpRequest'}
            resp = self.session.post(VALIDATE_CAPTCHA_URL, data=data, headers=headers, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            return result.get("success") is True
        except Exception as e:
            logger.error(f"Error validating CAPTCHA: {e}")
            return False

    def fetch_orders(self, orders_url: str) -> List[Dict[str, Any]]:
        """
        Fetch orders from the given orders URL (JSON endpoint).
        """
        try:
            headers = {'X-Requested-With': 'XMLHttpRequest'}
            params = {
                "draw": "1",
                "start": "0",
                "length": "200", # Fetch ample history
                "search[value]": "",
                "search[regex]": "false"
            }
            resp = self.session.get(orders_url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            orders = []
            for item in data.get('data', []):
                # Parse link
                link_html = item.get('case_no_order_link')
                doc_url = None
                if link_html:
                     soup = BeautifulSoup(link_html, 'html.parser')
                     a_tag = soup.find('a')
                     if a_tag and a_tag.get('href'):
                         doc_url = a_tag['href'].strip()
                
                # Date format is dd/mm/yyyy
                date_str = item.get('orddate')
                if date_str:
                    try:
                         # normalize to YYYY-MM-DD
                         dt = datetime.strptime(date_str, "%d/%m/%Y")
                         date_str = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass
                
                if doc_url:
                    orders.append({
                        "date": date_str,
                        "description": f"Order dated {date_str}",
                        "document_url": doc_url
                    })
            
            # Sort by date descending
            orders.sort(key=lambda x: x['date'] if x['date'] else "", reverse=True)
            return orders

        except Exception as e:
            logger.error(f"Error fetching orders from {orders_url}: {e}")
            return []

    @retry(
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_case_details(self, case_type: str, case_no: str, case_year: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch case status/details.
        Note: The Delhi HC search returns a list (table rows).
        """
        # 1. Get Session & Captcha
        captcha_code = self._get_initial_state()
        if not captcha_code:
            raise ValueError("Failed to retrieve CAPTCHA code")
            
        # 2. Validate Captcha
        if not self.validate_captcha(captcha_code):
            raise ValueError("CAPTCHA validation failed")

        # 3. Perform Search (DataTables Request)
        params = {
            "draw": "1",
            "start": "0",
            "length": "50",  # Get up to 50 results
            "case_type": case_type,
            "case_number": case_no,
            "case_year": case_year,
        }
        
        try:
            headers = {'X-Requested-With': 'XMLHttpRequest'}
            resp = self.session.get(SEARCH_URL, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            # The response format for DataTables is { draw: X, recordsTotal: Y, recordsFiltered: Z, data: [...] }
            rows = data.get('data', [])
            return self._parse_results(rows)
            
        except Exception as e:
            logger.error(f"Error searching cases: {e}")
            return None

    def _parse_results(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse the raw DataTables rows into a cleaner format.
        Columns: 'ctype', 'pet', 'orderdate' (which seems to be Listing Date / Court No.)
        """
        parsed_cases = []
        for row in rows:
            # row is a dict like {'DT_RowIndex': 1, 'ctype': 'HTML...', 'pet': 'HTML...', 'orderdate': 'HTML...'}
            
            case_info = {
                "orders": []
            }
            
            # Parse 'ctype' column (Diary No. / Case No.[STATUS])
            if 'ctype' in row:
                soup = BeautifulSoup(row['ctype'], 'html.parser')
                text = soup.get_text(" ", strip=True)
                case_info['case_details_raw'] = text
                
                # Extract Case No and Status
                match = re.search(r'(.*?)\[(.*?)\]', text)
                if match:
                    case_info['case_no'] = match.group(1).strip()
                    case_info['status'] = match.group(2).strip()
                else:
                    case_info['case_no'] = text

                # Extract Orders URL
                orders_link = soup.find('a', string=re.compile(r"Click here for Orders", re.IGNORECASE))
                if orders_link and orders_link.get('href'):
                    orders_url = orders_link['href'].strip()
                    # Fetch orders if URL is found
                    case_info['orders'] = self.fetch_orders(orders_url)
                    
            # Parse 'pet' column (Petitioner Vs. Respondent)
            if 'pet' in row:
                soup = BeautifulSoup(row['pet'], 'html.parser')
                case_info['parties'] = soup.get_text(" ", strip=True)
                # Split Pet vs Res
                parts = re.split(r'\s+VS\.?\s+', case_info['parties'], flags=re.IGNORECASE)
                if len(parts) >= 2:
                    case_info['petitioner'] = parts[0].strip()
                    case_info['respondent'] = parts[1].strip()
                else:
                    case_info['petitioner'] = case_info['parties']
                
            # Parse 'orderdate' column (Listing Date / Court No.)
            if 'orderdate' in row:
                soup = BeautifulSoup(row['orderdate'], 'html.parser')
                text = soup.get_text(" ", strip=True)
                case_info['listing_details'] = text
                
                # Extract Date (dd/mm/yyyy)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
                if date_match:
                    case_info['next_hearing_date'] = date_match.group(1)
                
                # Extract Court No
                court_match = re.search(r'COURT NO\s*:\s*(\d+)', text, re.IGNORECASE)
                if court_match:
                    case_info['court_no'] = court_match.group(1)

            parsed_cases.append(case_info)
            
        return parsed_cases

# Global instance
_service = DelhiHCService()

def get_delhi_case_details(case_type: str, case_no: str, case_year: str):
    return _service.fetch_case_details(case_type, case_no, case_year)

async def persist_orders_to_storage(
    orders: list[dict] | None,
    case_id: str | None = None,
) -> list[dict] | None:
    """
    Upload scraped Delhi HC order documents to storage and update their URLs.
    """
    # Delhi HC order URLs might be simple GETs but let's check.
    # The URL is like: https://delhihighcourt.nic.in/app/showlogo/TOKEN/YEAR
    # It likely requires no special headers if opened in new tab, but maybe user-agent.
    
    def _fetch_order_document(url: str, referer: str | None = None) -> requests.Response:
        headers = {
             'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
        }
        if referer:
            headers["Referer"] = referer
        return requests.get(url, timeout=60, headers=headers)

    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        referer=f"{BASE_URL}/",
    )

if __name__ == "__main__":
    import asyncio
    import json
    logging.basicConfig(level=logging.INFO)
    
    results = get_delhi_case_details("W.P.(C)", "533", "2025")
    print(json.dumps(results, indent=2))
    
    # Optional: Test persist orders if needed (requires Supabase env vars)
    # if results and results[0]['orders']:
    #     asyncio.run(persist_orders_to_storage(results[0]['orders'], case_id="TEST_CASE_ID"))




