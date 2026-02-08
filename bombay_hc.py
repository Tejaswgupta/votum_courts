
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import ddddocr
import fitz
import requests
from fastapi import APIRouter, Form, HTTPException
from supabase_client import get_supabase_client
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

try:
    from .order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage
except ImportError:
    from order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)

# Router for side-effects/cron usage if needed
router = APIRouter()

CRON_JOB_RUNS_TABLE = "cron_job_runs"

BASE_URL = "https://bombayhighcourt.gov.in/bhc"
SEARCH_URL = f"{BASE_URL}/case-status-new"
CASE_TYPE_URL = f"{BASE_URL}/get-case-types-new"
SEARCH_API_URL = f"{BASE_URL}/get-case-status-by-caseno-new"

# Cause List Constants
OLD_BASE_URL = "https://bombayhighcourt.nic.in"
CAUSE_LIST_PDF_PAGE = f"{OLD_BASE_URL}/netbdpdf.php"
CAPTCHA_URL = f"{OLD_BASE_URL}/captcha.php"

CASE_NO_PATTERN = re.compile(r"\b(?:[A-Z]{1,6}/)?[A-Z]{1,10}/\d{1,7}/\d{4}\b")


def _normalize_case_token(case_no: str) -> str:
    return re.sub(r"\s+", "", (case_no or "").upper())


def _case_tail(case_no: str) -> str:
    token = _normalize_case_token(case_no)
    # Split by / or - or space
    parts = re.split(r"[/-\s]", token)
    if len(parts) >= 3:
        return "/".join(parts[-3:])
    return token


class BombayHCService:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
        }
        self.ocr = ddddocr.DdddOcr(show_ad=False)
        self.case_types_map = {} # Name -> Code (e.g. "WP" -> "560")
        self.case_types_path = Path(__file__).with_name("bombay_case_types.json")

    def _refresh_session(self) -> Dict[str, str]:
        """Visit search page to get session and tokens."""
        self.session.headers.update({'X-Requested-With': 'XMLHttpRequest'})
        try:
            resp = self.session.get(SEARCH_URL, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, 'html.parser')
            form = soup.find('form', id='getCaseStatusByCaseNo')
            if not form:
                # Try causelist page if search page doesn't have it
                resp = self.session.get(f"{BASE_URL}/causelistFinal", timeout=30)
                soup = BeautifulSoup(resp.content, 'html.parser')
                token = soup.find('meta', {'name': 'csrf-token'}).get('content')
                # For causelistFinal, form_secret is in the forms
                form = soup.find('form', class_='causelist_form')
                if not form:
                     return {'_token': token, 'form_secret': ''}
                secret = form.find('input', {'name': 'form_secret'}).get('value')
                return {'_token': token, 'form_secret': secret}

            token = form.find('input', {'name': '_token'}).get('value')
            secret = form.find('input', {'name': 'form_secret'}).get('value')
            
            return {'_token': token, 'form_secret': secret}
        except Exception as e:
            logger.error(f"Failed to refresh session: {e}")
            raise

    def solve_captcha(self) -> Optional[str]:
        """Download and solve CAPTCHA for the old site."""
        try:
            # First visit the page to get tokens
            resp = self.session.get(CAUSE_LIST_PDF_PAGE, timeout=30)
            soup = BeautifulSoup(resp.content, 'html.parser')
            csrf_name_tag = soup.find('input', {'name': 'CSRFName'})
            if not csrf_name_tag:
                return None
            
            csrf_name = csrf_name_tag.get('value')
            csrf_token = soup.find('input', {'name': 'CSRFToken'}).get('value')
            
            # Get Captcha
            resp_cap = self.session.get(CAPTCHA_URL, timeout=10)
            if resp_cap.status_code == 200:
                captcha_text = self.ocr.classification(resp_cap.content)
                return captcha_text, csrf_name, csrf_token
        except Exception as e:
            logger.error(f"Error solving CAPTCHA: {e}")
        return None

    def fetch_cause_list_pdf_bytes(self, listing_date: datetime, bench: str = "B") -> bytes:
        """
        Fetch cause list PDF for a given date and bench.
        Bench codes: B=Bombay, N=Nagpur, A=Aurangabad, G=Goa, K=Kolhapur
        """
        captcha_res = self.solve_captcha()
        if not captcha_res:
            raise ValueError("Failed to solve CAPTCHA or get tokens")
        
        captcha_text, csrf_name, csrf_token = captcha_res
        
        payload = {
            'CSRFName': csrf_name,
            'CSRFToken': csrf_token,
            'm_juris': bench,
            'm_causedt': listing_date.strftime("%d/%m/%Y"),
            'captcha_code': captcha_text,
            'captchaflg': '',
            'usubmit': 'GO' # Trigger the submission
        }
        
        # The GO button calls go('usubmit')
        # We might need to handle redirects if the PDF is served via another page
        resp = self.session.post(CAUSE_LIST_PDF_PAGE, data=payload, timeout=60)
        resp.raise_for_status()
        
        if 'pdf' in resp.headers.get('Content-Type', '').lower():
            return resp.content
        
        # If not a PDF, check if it's a page with PDF links
        soup = BeautifulSoup(resp.content, 'html.parser')
        pdf_links = [a['href'] for a in soup.find_all('a', href=True) if '.pdf' in a['href'].lower()]
        if pdf_links:
            # Download the first PDF found
            pdf_url = urljoin(OLD_BASE_URL, pdf_links[0])
            resp_pdf = self.session.get(pdf_url, timeout=60)
            resp_pdf.raise_for_status()
            return resp_pdf.content
            
        raise ValueError(f"Failed to fetch cause list PDF. Response type: {resp.headers.get('Content-Type')}")

    def parse_cause_list_pdf(self, pdf_path: str) -> List[Dict[str, Any]]:
        """
        Parse Bombay HC cause-list PDF and extract structured entries.
        """
        entries: List[Dict[str, Any]] = []

        with fitz.open(pdf_path) as doc:
            for page_idx in range(doc.page_count):
                page = doc[page_idx]
                text = page.get_text("text")
                
                # Bombay HC PDFs are often structured in blocks or tables.
                # A simple approach is to find all case numbers and their surrounding text.
                lines = text.split('\n')
                current_entry = None
                
                for i, line in enumerate(lines):
                    cleaned_line = line.strip()
                    if not cleaned_line:
                        continue
                    
                    # Detect case numbers like WP/123/2023 or ASWP/123/2023
                    case_matches = CASE_NO_PATTERN.findall(cleaned_line)
                    if case_matches:
                        if current_entry:
                            entries.append(self._finalize_entry(current_entry))
                        
                        current_entry = {
                            "item_no": None, # Will try to extract
                            "page_no": page_idx + 1,
                            "case_nos": [_normalize_case_token(m) for m in case_matches],
                            "raw_lines": [cleaned_line],
                            "text": cleaned_line
                        }
                        
                        # Look for item number in previous lines
                        if i > 0:
                            prev_line = lines[i-1].strip()
                            if re.match(r"^\d{1,4}$", prev_line):
                                current_entry["item_no"] = prev_line
                    elif current_entry:
                        current_entry["raw_lines"].append(cleaned_line)
                        current_entry["text"] += "\n" + cleaned_line
                
                if current_entry:
                    entries.append(self._finalize_entry(current_entry))
                    current_entry = None

        return [e for e in entries if e.get("case_nos")]

    def _finalize_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Finalize an entry with hash and basic party names."""
        text = "\n".join(entry["raw_lines"]).strip()
        entry_hash = hashlib.sha256(f"{entry.get('item_no')}|{entry.get('page_no')}|{text}".encode("utf-8")).hexdigest()
        
        # Simple party name extraction (look for V/S)
        party_names = None
        petitioner = None
        respondent = None
        for line in entry["raw_lines"]:
            if "V/S" in line.upper() or " VS " in line.upper():
                party_names = line
                parts = re.split(r"V/S| VS ", line, flags=re.IGNORECASE)
                if len(parts) >= 2:
                    petitioner = parts[0].strip()
                    respondent = parts[1].strip()
                break
        
        return {
            "item_no": entry.get("item_no"),
            "page_no": entry.get("page_no"),
            "case_no": entry["case_nos"][0] if entry["case_nos"] else None,
            "case_nos": entry["case_nos"],
            "petitioner": petitioner,
            "respondent": respondent,
            "party_names": party_names,
            "text": text,
            "entry_hash": entry_hash,
        }

    def find_case_entries(self, pdf_path: str, registration_no: str) -> List[Dict[str, Any]]:
        """
        Find cause-list entries that match a registration/case number.
        """
        target_tail = _case_tail(registration_no)
        parsed = self.parse_cause_list_pdf(pdf_path)
        if not target_tail:
            return parsed

        matched_entries: List[Dict[str, Any]] = []
        for entry in parsed:
            case_nos = entry.get("case_nos") or []
            tails = {_case_tail(case_no) for case_no in case_nos if case_no}
            if target_tail in tails:
                matched_entries.append(entry)
        return matched_entries

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

def get_bombay_cause_list_pdf(listing_date: datetime, bench: str = "B") -> bytes:
    return _service.fetch_cause_list_pdf_bytes(listing_date, bench=bench)

def parse_bombay_cause_list_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    return _service.parse_cause_list_pdf(pdf_path)

def find_bombay_case_entries(pdf_path: str, registration_no: str) -> List[Dict[str, Any]]:
    return _service.find_case_entries(pdf_path, registration_no)


def _create_cron_job_run(supabase, job_name: str, metadata: Dict[str, Any]) -> str | None:
    try:
        payload = {"job_name": job_name, "status": "running", "metadata": metadata}
        result = supabase.table(CRON_JOB_RUNS_TABLE).insert(payload).execute()
        if result.data:
            return result.data[0].get("id")
    except Exception as exc:
        logger.warning("Failed to create cron job run: %s", exc)
    return None


def _finish_cron_job_run(
    supabase,
    run_id: str | None,
    status: str,
    summary: Dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    if not run_id:
        return
    payload: Dict[str, Any] = {
        "status": status,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    if summary is not None:
        payload["summary"] = summary
    if error:
        payload["error"] = error
    try:
        supabase.table(CRON_JOB_RUNS_TABLE).update(payload).eq("id", run_id).execute()
    except Exception as exc:
        logger.warning("Failed to update cron job run %s: %s", run_id, exc)


@router.post("/bombay_cause_list/sync")
async def sync_bombay_cause_list(
    listing_date: Optional[str] = Form(None),
    bench: str = Form("B"),
    dry_run: bool = Form(False),
    limit: Optional[int] = Form(None),
):
    """
    Fetch Bombay HC cause list PDF, extract matching case rows,
    and store extracted text.
    """
    supabase = get_supabase_client()
    run_id = _create_cron_job_run(
        supabase,
        "bombay_cause_list_sync",
        {"listing_date": listing_date, "bench": bench, "dry_run": dry_run, "limit": limit},
    )
    run_status = "failed"
    run_error: str | None = None
    run_summary: Dict[str, Any] | None = None
    pdf_path: str | None = None

    try:
        if listing_date:
            target_date = datetime.strptime(listing_date, "%Y-%m-%d")
        else:
            target_date = datetime.now() + timedelta(days=1)

        try:
            pdf_bytes = get_bombay_cause_list_pdf(target_date, bench=bench)
        except Exception as e:
            run_error = f"Failed to fetch cause list PDF: {str(e)}"
            raise HTTPException(
                status_code=502,
                detail=f"Failed to fetch cause list PDF: {str(e)}",
            )

        with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            tmp_pdf.write(pdf_bytes)
            pdf_path = tmp_pdf.name

        # For now, just parse and log
        entries = parse_bombay_cause_list_pdf(pdf_path)
        
        run_status = "completed"
        run_summary = {
            "total_entries": len(entries),
            "date": target_date.strftime("%Y-%m-%d"),
            "bench": bench
        }
        
        return {"status": "success", "summary": run_summary}

    except Exception as exc:
        if run_error is None:
            run_error = str(exc)
        raise
    finally:
        _finish_cron_job_run(
            supabase,
            run_id,
            run_status,
            summary=run_summary,
            error=run_error,
        )
        if pdf_path and os.path.exists(pdf_path):
            os.remove(pdf_path)


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
    # Test Case Details
    # print("Testing AS...")
    # print(json.dumps(get_bombay_case_details("1", "1", "2025", side="1"), indent=2, default=str))
    
    # Test Cause List
    print("Testing Cause List Fetching...")
    try:
        pdf_bytes = get_bombay_cause_list_pdf(datetime(2026, 2, 9), bench="B")
        with open("bombay_cl_test.pdf", "wb") as f:
            f.write(pdf_bytes)
        print("Successfully fetched cause list PDF.")
        
        entries = parse_bombay_cause_list_pdf("bombay_cl_test.pdf")
        print(f"Found {len(entries)} entries in PDF.")
        if entries:
            print("First entry:", json.dumps(entries[0], indent=2))
    except Exception as e:
        print(f"Cause list test failed: {e}")
