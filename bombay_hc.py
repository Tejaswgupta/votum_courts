
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

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

BASE_URL = "https://bombayhighcourt.gov.in/bhc"
SEARCH_URL = f"{BASE_URL}/case-status-new"
CASE_TYPE_URL = f"{BASE_URL}/get-case-types-new"
SEARCH_API_URL = f"{BASE_URL}/get-case-status-by-caseno-new"

class BombayHCService:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.case_types_map = {} # Name -> Code (e.g. "WP" -> "560")
        self.case_types_path = Path(__file__).with_name("bombay_case_types.json")

    def _refresh_session(self) -> Dict[str, str]:
        """Visit search page to get session and tokens."""
        try:
            resp = self.session.get(SEARCH_URL, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, 'html.parser')
            form = soup.find('form', id='getCaseStatusByCaseNo')
            if not form:
                raise ValueError("Search form not found")
            
            token = form.find('input', {'name': '_token'}).get('value')
            secret = form.find('input', {'name': 'form_secret'}).get('value')
            
            return {'_token': token, 'form_secret': secret}
        except Exception as e:
            logger.error(f"Failed to refresh session: {e}")
            raise

    def _clean_text(self, text: str) -> Optional[str]:
        if not text:
            return None
        # Remove newlines and collapse spaces
        t = re.sub(r'\s+', ' ', text).strip()
        if t in ['—', '-', '', 'NA']:
            return None
        return t

    def _parse_date(self, date_str: str) -> Optional[str]:
        if not date_str or '—' in date_str or '-' == date_str.strip():
            return None
        try:
            # Format usually dd-mm-yyyy or similar
            return datetime.strptime(date_str.strip(), "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _extract_label_value(self, soup, label_text):
        """Helper to find 'Label' ...... 'Value' structure in the HTML"""
        # The HTML uses <b>Label</b> ... value structure often in divs
        # <div class="col-xxl-4"><b>Label</b></div>
        # <div class="col-xxl-8">Value</div>
        
        # Find b tag with label
        label_b = soup.find('b', string=lambda s: s and label_text.lower() in s.lower())
        if label_b:
            # Go up to col-xxl-4 div
            label_col = label_b.find_parent('div')
            if label_col:
                # Find next sibling div (value col)
                value_col = label_col.find_next_sibling('div')
                if value_col:
                    return value_col.get_text(strip=True)
        return None

    def _parse_html_response(self, html_content: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        result = {
            "status": None,
            "cnr_no": None,
            "filing_no": None,
            "registration_no": None, # Same as case no usually
            "registration_date": None,
            "filing_date": None,
            "case_type": None,
            "case_no": None,
            "case_year": None,
            "pet_name": [],
            "res_name": [],
            "pet_advocates": [],
            "res_advocates": [],
            "judges": None, # Not always available in main details
            "court_name": "Bombay High Court",
            "bench_name": None,
            "district": None,
            "orders": [],
            "history": [] 
        }

        # 1. Main Details
        # Filing Number WP/2/2023 ... with CNR No. HCBM020134702023 ... filed on 12-05-2023
        # Use text-based search for the header div
        header_div = soup.find(lambda tag: tag.name == 'div' and tag.get('class') and 'border-bottom' in tag.get('class') and "CNR No." in tag.text)
        
        if header_div:
            text = header_div.get_text(" ", strip=True)
            # Extract CNR
            cnr_match = re.search(r"CNR No[\.:]?\s*([A-Z0-9]+)", text, re.IGNORECASE)
            if cnr_match:
                result['cnr_no'] = cnr_match.group(1)
            
            # Extract Filing Date
            date_match = re.search(r"filed on\s*(\d{2}-\d{2}-\d{4})", text, re.IGNORECASE)
            if date_match:
                result['filing_date'] = self._parse_date(date_match.group(1))

        # Structured fields
        result['filing_no'] = self._clean_text(self._extract_label_value(soup, "Filing Number"))
        result['registration_date'] = self._parse_date(self._extract_label_value(soup, "Registration Date"))
        result['status'] = self._clean_text(self._extract_label_value(soup, "Status"))
        
        # Petitioner
        pet_text = self._extract_label_value(soup, "Petitioner")
        if pet_text:
            result['pet_name'] = [pet_text]
            
        # Respondent
        res_text = self._extract_label_value(soup, "Respondent")
        if res_text:
            result['res_name'] = [res_text]
            
        # Advocates
        pet_adv = self._extract_label_value(soup, "Petitioner's Advocate")
        if pet_adv:
            result['pet_advocates'] = [pet_adv]
            
        res_adv = self._extract_label_value(soup, "Respondent's Advocate")
        if res_adv:
            result['res_advocates'] = [res_adv]

        # Case No parsing (from Filing No if Reg No is missing?)
        # "WP - 2 - 2023" or "WP 2 2023"
        if result['filing_no']:
            # Replace - with space and split
            parts = result['filing_no'].replace('-', ' ').split()
            if len(parts) >= 3:
                result['case_type'] = parts[0].strip()
                result['case_no'] = parts[1].strip()
                result['case_year'] = parts[2].strip()
                result['registration_no'] = f"{result['case_type']}/{result['case_no']}/{result['case_year']}"

        # Clean lists
        result['pet_advocates'] = [x for x in [self._clean_text(a) for a in result['pet_advocates']] if x]
        result['res_advocates'] = [x for x in [self._clean_text(a) for a in result['res_advocates']] if x]


        # Orders
        orders_tab = soup.find('div', id='CaseNoOrders')
        if orders_tab:
            rows = orders_tab.find_all('tr')
            for row in rows[1:]: # Skip header
                cols = row.find_all('td')
                if len(cols) >= 3:
                    coram = cols[1].get_text(strip=True)
                    date_val = self._parse_date(cols[2].get_text(strip=True))
                    
                    # Check for links (PDFs)
                    doc_url = None
                    link = row.find('a', href=True)
                    if link:
                        doc_url = link['href']
                        if not doc_url.startswith('http'):
                            doc_url = urljoin(BASE_URL, doc_url)

                    order = {
                        "date": date_val,
                        "description": f"Order by {coram}",
                        "judge": coram,
                        "document_url": doc_url
                    }
                    result['orders'].append(order)

        return result

    @retry(
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_case_details(
        self,
        case_type_name: str,
        case_no: str,
        case_year: str,
        side: str = "AS",
        stamp: str = "Register"
    ) -> Optional[Dict[str, Any]]:
        
        # 1. Get Tokens
        tokens = self._refresh_session()
        
        if case_type_name.isdigit():
            case_code = case_type_name
        else:
            logger.error(f"Case type '{case_type_name}' not found for side {side}.")
            return None

        # 3. POST Search
        payload = {
            '_token': tokens['_token'],
            'form_secret': tokens['form_secret'],
            'side': '1',
            'Stamp': 'R',
            'case_type': case_code,
            'case_no': str(case_no),
            'year': str(case_year),
        }
        print('----')
        print(payload)
        print('----')
        
        resp = self.session.post(SEARCH_API_URL, data=payload, timeout=30)
        resp.raise_for_status()
        
        json_resp = resp.json()
        
        if json_resp.get('status') is True:
            html = json_resp.get('page')
            if html:
                return self._parse_html_response(html)
        else:
            logger.warning(f"Search failed for {case_type_name}/{case_no}/{case_year}: {json_resp.get('message')}")
            return None
        
        return None

# Global instance
_service = BombayHCService()

def get_bombay_case_details(
    case_type: str,
    case_no: str,
    case_year: str,
    side: str = "AS",
    stamp: str = "Register"
):
    return _service.fetch_case_details(case_type, case_no, case_year, side=side, stamp=stamp)

def _fetch_order_document(url: str, referer: Optional[str] = None) -> requests.Response:
    """Helper to fetch order documents."""
    headers = {}
    if referer:
        headers["Referer"] = referer
    return _service.session.get(url, timeout=30, headers=headers)

async def persist_orders_to_storage(
    orders: Optional[List[dict]],
    case_id: Optional[str] = None,
) -> Optional[List[dict]]:
    """
    Upload scraped Bombay HC order documents to storage and update their URLs.
    """
    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        referer=SEARCH_URL,
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Test AS
    print("Testing AS...")
    print(json.dumps(get_bombay_case_details("1", "1", "2025", side="1"), indent=2, default=str))
    
    # Test OS (if needed, but usually we just want to see it works)
