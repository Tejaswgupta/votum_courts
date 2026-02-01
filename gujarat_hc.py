import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import ddddocr
import requests
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

try:
    from .order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage
except ImportError:
    from order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)

BASE_URL = "https://gujarathc-casestatus.nic.in/gujarathc"
CASE_TYPE_URL = f"{BASE_URL}/GetCaseTypeDataOnLoad"
CAPTCHA_URL = f"{BASE_URL}/CaptchaServlet?ct=S&tm={{}}"
DATA_URL = f"{BASE_URL}/GetData"

class GujaratHCService:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0'
        }
        self.ocr = ddddocr.DdddOcr(show_ad=False)
        self.case_types_map = {}
        self.case_types_path = Path(__file__).with_name("gujarat_case_types.json")

    def _refresh_session(self):
        """Visit main page to establish session/cookies."""
        try:
            self.session.get(f"{BASE_URL}/", timeout=30)
        except Exception as e:
            logger.warning(f"Failed to refresh session: {e}")

    def solve_captcha(self) -> Optional[str]:
        """Download and solve CAPTCHA."""
        try:
            ts = int(time.time() * 1000)
            url = CAPTCHA_URL.format(ts)
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                res = self.ocr.classification(resp.content)
                return res
        except Exception as e:
            logger.error(f"Error solving CAPTCHA: {e}")
        return None

    @retry(
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def get_case_types(self) -> Dict[str, str]:
        """Fetch and cache case types (Name -> Code)."""
        if self.case_types_map:
            return self.case_types_map

        if self.case_types_path.exists():
            try:
                with self.case_types_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and data:
                    self.case_types_map = data
                    return self.case_types_map
                logger.warning("Case types file is empty or invalid; refetching.")
            except Exception as e:
                logger.warning(f"Failed to read case types file: {e}; refetching.")

        self._refresh_session()
        try:
            resp = self.session.post(CASE_TYPE_URL, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            # Parse case types
            # Structure: finaldata[0].casetypearray[].Civil[].{casetype, casecode}
            groups = data.get('finaldata', [])[0].get('casetypearray', [])
            mapping = {}
            for group in groups:
                for category in ['Civil', 'Criminal', 'OJ']:
                    if category in group:
                        for item in group[category]:
                            name = item.get('casetype')
                            code = item.get('casecode')
                            if name and code:
                                mapping[name] = code
            
            self.case_types_map = mapping
            try:
                with self.case_types_path.open("w", encoding="utf-8") as f:
                    json.dump(self.case_types_map, f, ensure_ascii=True, indent=2, sort_keys=True)
            except Exception as e:
                logger.warning(f"Failed to persist case types file: {e}")
            return mapping
        except Exception as e:
            logger.error(f"Failed to fetch case types: {e}")
            raise

    def _parse_date(self, date_str: str) -> Optional[str]:
        if not date_str or date_str.strip() in ['-', '', 'NA']:
            return None
        try:
            return datetime.strptime(date_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def fetch_order_document(self, order_params: Dict[str, str]) -> requests.Response:
        """
        Fetch the actual PDF content for an order.
        order_params should contain ccin_no, order_no, order_date, flag, casedetail, nc.
        """
        url = f"{BASE_URL}/OrderHistoryViewDownload"
        data = {
            'ccin_no': order_params.get('ccin_no'),
            'order_no': order_params.get('order_no'),
            'order_date': order_params.get('order_date'),
            'flag': order_params.get('flag', 'v'),
            'casedetail': order_params.get('casedetail'),
            'nc': order_params.get('nc', '-'),
            'download_token_value_id': str(int(time.time() * 1000))
        }
        return self.session.post(url, data=data, timeout=30)

    def _parse_details(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Parse the JSON response into a normalized format."""
        
        result = {
            "status": None,
            "cin_no": None,
            "registration_no": None,
            "registration_date": None,
            "filing_no": None,
            "filing_date": None,
            "case_type": None,
            "case_no": None,
            "case_year": None,
            "pet_name": [],
            "res_name": [],
            "pet_advocates": [],
            "res_advocates": [],
            "judges": None,
            "court_name": "Gujarat High Court",
            "bench_name": None,
            "district": None,
            "first_hearing_date": None,
            "next_hearing_date": None,
            "decision_date": None,
            "orders": [],
            "history": [],
            "raw_data": data  # Keep raw data for debugging/completeness
        }

        # 4. Main Details
        if len(data) > 4 and 'maindetails' in data[4]:
            main = data[4]['maindetails'][0]
            result['cin_no'] = main.get('ccin')
            result['status'] = main.get('casestatus')
            result['registration_date'] = self._parse_date(main.get('registration_date'))
            result['filing_no'] = main.get('stampnumber')
            result['filing_date'] = self._parse_date(main.get('presentdate')) # "Presented On"
            result['case_type'] = main.get('casetype')
            result['case_no'] = main.get('casenumber')
            result['case_year'] = main.get('caseyear')
            result['bench_name'] = main.get('benchname')
            result['district'] = main.get('districtname')
            result['judges'] = main.get('judges')
            result['next_hearing_date'] = self._parse_date(main.get('listingdate')) # Often listingdate is next date
            result['decision_date'] = self._parse_date(main.get('disposaldate'))
            
            # Format registration number: TYPE/NO/YEAR
            if result['case_type'] and result['case_no'] and result['case_year']:
                result['registration_no'] = f"{result['case_type']}/{result['case_no']}/{result['case_year']}"

        # 0. Litigant (Petitioner)
        if len(data) > 0 and 'litigant' in data[0]:
            for item in data[0]['litigant']:
                name = item.get('litigantname')
                if name:
                    result['pet_name'].append(name)

        # 1. Respondent
        if len(data) > 1 and 'respondant' in data[1]:
            for item in data[1]['respondant']:
                name = item.get('respondantname')
                if name:
                    result['res_name'].append(name)

        # 2. Advocate
        if len(data) > 2 and 'advocate' in data[2]:
            for item in data[2]['advocate']:
                adv_name = item.get('advocatename')
                l_type = item.get('litiganttypecode') # 1=Pet, 2=Res
                if l_type == '1':
                    result['pet_advocates'].append(adv_name)
                elif l_type == '2':
                    result['res_advocates'].append(adv_name)

        # 10. Court Proceedings (History)
        if len(data) > 10 and 'linkedmatterscp' in data[10]:
            for item in data[10]['linkedmatterscp']:
                entry = {
                    "business_date": self._parse_date(item.get('PROCEEDINGDATElmcp')),
                    "hearing_date": self._parse_date(item.get('PROCEEDINGDATElmcp')), # Assuming same
                    "judge": item.get('JUDGESlmcp'),
                    "purpose": item.get('STAGENAMElmcp'),
                    "result": item.get('ACTIONNAMElmcp')
                }
                result['history'].append(entry)

        # 11. Order History
        if len(data) > 11 and 'orderhistory' in data[11]:
            for item in data[11]['orderhistory']:
                # Construct logical document_url with parameters
                params = {
                    "ccin_no": item.get('ccinoh'),
                    "order_no": item.get('ordernooh'),
                    "order_date": item.get('orderdate'),
                    "flag": 'v',
                    "casedetail": item.get('descriptionoh'),
                    "nc": item.get('nc', '-')
                }
                # Create a pseudo-URL that contains all necessary info
                param_str = "&".join([f"{k}={v}" for k, v in params.items()])
                doc_url = f"{BASE_URL}/OrderHistoryViewDownload?{param_str}"

                order = {
                    "date": self._parse_date(item.get('orderdate')),
                    "description": f"{item.get('descriptionoh')} | {item.get('judgesoh')} | {item.get('orderdate')}",
                    "judge": item.get('judgesoh'),
                    "order_no": item.get('ordernooh'),
                    "ccin": item.get('ccinoh'),
                    "document_url": doc_url
                }
                result['orders'].append(order)

        return result

    @retry(
        retry=retry_if_exception_type((requests.RequestException, ValueError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_case_details(self, case_type_name: str, case_no: str, case_year: str) -> Optional[Dict[str, Any]]:
        """
        Fetch case details by Case Type (Name), Number, and Year.
        e.g. fetch_case_details("SCA", "7966", "2025")
        """
        self._refresh_session()
        
        # 1. Get Case Code
        types = self.get_case_types()
        case_code = types.get(case_type_name.upper()) or types.get(case_type_name)
        if not case_code:
            logger.error(f"Case type '{case_type_name}' not found.")
            # Fallback: maybe the user passed the code directly?
            if case_type_name.isdigit():
                 case_code = case_type_name
            else:
                 return None

        # 2. Solve Captcha
        captcha = self.solve_captcha()
        if not captcha:
            raise ValueError("Failed to solve CAPTCHA")

        # 3. Construct GENCCIN
        # Format: R#<case_code>#<case_no>#<case_year>
        # Ensure 3 digit case code (padded), 5 digit case no (padded) - handled by server usually but JS does padding
        # JS: while (casecode.length < 3) casecode = "0" + casecode;
        # JS: while (casenumber.length < 5) casenumber = "0" + casenumber;
        
        case_code = str(case_code).zfill(3)
        
        genccin = f"R#{case_code}#{case_no}#{case_year}"
        
        # 4. Fetch Data
        data = {
            'ccin': genccin,
            'servicecode': '1',
            'challengeString': captcha
        }
        
        resp = self.session.post(DATA_URL, data=data, timeout=30)
        resp.raise_for_status()
        
        json_resp = resp.json()
        
        # Check for errors in response
        if 'finaldata' in json_resp:
            error_msg = json_resp['finaldata'][0].get('ERROR')
            if error_msg:
                if "captcha" in error_msg.lower():
                    logger.warning(f"Server returned error: {error_msg}. Retrying...")
                    raise ValueError(f"Server error: {error_msg}")
                logger.error(f"Server returned error: {error_msg}")
                return None
             
        if 'data' in json_resp:
            parsed = self._parse_details(json_resp['data'])
            if parsed and not parsed.get("registration_no"):
                logger.warning("Missing registration_no for case %s/%s/%s; retrying.", case_type_name, case_no, case_year)
                raise ValueError("Missing registration_no")
            return parsed
        
        return None

# Global instance
_service = GujaratHCService()

def get_gujarat_case_details(case_type: str, case_no: str, case_year: str):
    return _service.fetch_case_details(case_type, case_no, case_year)

def _fetch_order_document(url: str, referer: str | None = None) -> requests.Response:
    """
    Helper to fetch order documents, handling both regular URLs and the pseudo-URLs
    generated for OrderHistoryViewDownload.
    """
    if "OrderHistoryViewDownload?" in url:
        parsed = urlparse(url)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        return _service.fetch_order_document(params)
    
    headers = {}
    if referer:
        headers["Referer"] = referer
    return _service.session.get(url, timeout=30, headers=headers)

async def persist_orders_to_storage(
    orders: list[dict] | None,
    case_id: str | None = None,
) -> list[dict] | None:
    """
    Upload scraped Gujarat HC order documents to storage and update their URLs.
    """
    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        referer=f"{BASE_URL}/",
    )

if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(get_gujarat_case_details("21", "7966", "2025")))
