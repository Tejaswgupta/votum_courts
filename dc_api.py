import html
import json
import logging
import os
import re
import time
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import ddddocr
import fitz
import requests
import urllib3
from bs4 import BeautifulSoup

try:
    from .order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage
except ImportError:
    from order_storage import \
        persist_orders_to_storage as _persist_orders_to_storage

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

# Initialize OCR once at module level
_ocr = ddddocr.DdddOcr(show_ad=False)

DC_CASE_NO_PATTERN = re.compile(
    r"\b(?:[A-Z0-9.()-]{1,20}/)?[A-Z0-9.()-]{1,20}/\d{1,8}/\d{2,4}\b"
)


def _normalize_case_token(case_no: str) -> str:
    return re.sub(r"\s+", "", (case_no or "").upper())


def _case_tail(case_no: str) -> str:
    token = _normalize_case_token(case_no)
    parts = token.split("/")
    if len(parts) >= 3:
        return "/".join(parts[-3:])
    return token


def _clean_pdf_line(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if not cleaned:
        return ""
    if cleaned.startswith("Page ") and " of " in cleaned:
        return ""
    if cleaned.startswith("Created on "):
        return ""
    return cleaned


def parse_dc_cause_list_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Parse District Court cause-list PDF and extract entries carrying case numbers.

    Returns list with keys:
    - item_no
    - page_no
    - case_no (first detected)
    - case_nos (all detected in the entry)
    - text (raw merged text for the entry)
    """
    entries: List[Dict[str, Any]] = []

    with fitz.open(pdf_path) as doc:
        for page_idx in range(doc.page_count):
            page = doc[page_idx]
            lines: List[Dict[str, Any]] = []

            for block in page.get_text("dict").get("blocks", []):
                if block.get("type") != 0:
                    continue
                for line in block.get("lines", []):
                    x0, y0, _, _ = line.get("bbox", (0.0, 0.0, 0.0, 0.0))
                    line_text = "".join(span.get("text", "") for span in line.get("spans", []))
                    cleaned = _clean_pdf_line(line_text)
                    if not cleaned:
                        continue
                    lines.append({"x": float(x0), "y": float(y0), "text": cleaned})

            lines.sort(key=lambda item: (item["y"], item["x"]))
            if not lines:
                continue

            open_entry: Optional[Dict[str, Any]] = None
            for line in lines:
                txt = line["text"]
                start_match = re.match(r"^(\d{1,4})\s*[.)-]?\s+", txt)

                if start_match:
                    if open_entry:
                        entries.append(open_entry)
                    open_entry = {
                        "item_no": start_match.group(1),
                        "page_no": page_idx + 1,
                        "lines": [txt],
                    }
                elif open_entry:
                    open_entry["lines"].append(txt)
                else:
                    open_entry = {
                        "item_no": None,
                        "page_no": page_idx + 1,
                        "lines": [txt],
                    }

            if open_entry:
                entries.append(open_entry)

    parsed_entries: List[Dict[str, Any]] = []
    for entry in entries:
        raw_lines = entry.get("lines") or []
        text = "\n".join(raw_lines).strip()

        case_nos: List[str] = []
        seen = set()
        for line in raw_lines:
            normalized_line = re.sub(r"\s+", "", line.upper())
            for token in DC_CASE_NO_PATTERN.findall(normalized_line):
                normalized = _normalize_case_token(token)
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    case_nos.append(normalized)

        if not case_nos:
            continue

        parsed_entries.append(
            {
                "item_no": entry.get("item_no"),
                "page_no": entry.get("page_no"),
                "case_no": case_nos[0],
                "case_nos": case_nos,
                "text": text,
            }
        )

    return parsed_entries


def find_dc_case_entries(pdf_path: str, registration_no: str) -> List[Dict[str, Any]]:
    """
    Filter parsed District Court cause-list entries by registration/case number tail.
    E.g. R/SCA/4937/2022 <-> SCA/4937/2022.
    """
    target_tail = _case_tail(registration_no)
    parsed = parse_dc_cause_list_pdf(pdf_path)
    if not target_tail:
        return parsed

    matched_entries: List[Dict[str, Any]] = []
    for entry in parsed:
        case_nos = entry.get("case_nos") or []
        entry_tails = {_case_tail(case_no) for case_no in case_nos}
        if target_tail in entry_tails:
            matched_entries.append(entry)
    return matched_entries

class EcourtsWebScraper:
    def __init__(self):
        self.base_url = "https://services.ecourts.gov.in/ecourtindia_v6"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'X-Requested-With': 'XMLHttpRequest'
        })
        self.app_token = None
        # Use module-level OCR
        self.ocr = _ocr

    def initialize_session(self):
        """Fetches homepage to set up session and app_token"""
        try:
            url = f"{self.base_url}/?p=casestatus/index"
            response = self.session.get(url, verify=False, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            token_input = soup.find('input', {'id': 'app_token'})
            if token_input:
                self.app_token = token_input.get('value')
                # logger.info(f"Initialized session with App Token: {self.app_token}")
                return True
            else:
                logger.error("Could not find app_token in homepage")
                return False
        except Exception as e:
            logger.error(f"Failed to initialize session: {e}")
            return False

    def _post(self, url_suffix, data):
        if not self.app_token:
            if not self.initialize_session():
                raise Exception("Failed to initialize session")

        url = f"{self.base_url}/?p={url_suffix}"
        data['app_token'] = self.app_token
        data['ajax_req'] = 'true'
        
        response = self.session.post(url, data=data, verify=False, timeout=30)
        
        try:
            json_resp = response.json()
            if 'app_token' in json_resp:
                self.app_token = json_resp['app_token']
            return json_resp
        except:
            return {'status': False, 'msg': 'Invalid JSON response', 'raw': response.text}

    def get_states(self):
        # We can extract this from the homepage if needed, or use the _post endpoints if available.
        # But usually states are loaded on homepage.
        # For now, let's re-fetch homepage if we need the list, or cache it.
        # Assuming initialize_session has been called.
        # We can also parse it from the session's last response if we stored it, 
        # but for simplicity, let's request it again or assume caller knows the code.
        # Actually, let's implement parsing from homepage content.
        url = f"{self.base_url}/?p=casestatus/index"
        response = self.session.get(url, verify=False)
        soup = BeautifulSoup(response.content, 'html.parser')
        state_select = soup.find('select', {'id': 'sess_state_code'})
        states = []
        if state_select:
            for option in state_select.find_all('option'):
                if option.get('value') and option.get('value') != '0':
                    states.append({'code': option.get('value'), 'name': option.text.strip()})
        return states

    def get_districts(self, state_code):
        data = {'state_code': state_code}
        resp = self._post('casestatus/fillDistrict', data)
        if isinstance(resp, dict) and 'dist_list' in resp:
            soup = BeautifulSoup(resp['dist_list'], 'html.parser')
            districts = []
            for option in soup.find_all('option'):
                 if option.get('value') and option.get('value') != '0':
                    districts.append({'code': option.get('value'), 'name': option.text.strip()})
            return districts
        return []

    def get_court_complexes(self, state_code, dist_code):
        data = {'state_code': state_code, 'dist_code': dist_code}
        resp = self._post('casestatus/fillcomplex', data)
        if isinstance(resp, dict) and 'complex_list' in resp:
            soup = BeautifulSoup(resp['complex_list'], 'html.parser')
            complexes = []
            for option in soup.find_all('option'):
                 if option.get('value') and option.get('value') != '0':
                    complexes.append({'code': option.get('value'), 'name': option.text.strip()})
            return complexes
        return []

    def get_establishments(self, state_code, dist_code, court_complex_code_full):
        parts = court_complex_code_full.split('@')
        complex_code = parts[0]
        
        data = {
            'state_code': state_code,
            'dist_code': dist_code,
            'court_complex_code': complex_code
        }
        resp = self._post('casestatus/fillCourtEstablishment', data)
        if isinstance(resp, dict) and 'establishment_list' in resp:
             soup = BeautifulSoup(resp['establishment_list'], 'html.parser')
             est = []
             for option in soup.find_all('option'):
                 if option.get('value') and option.get('value') != '0':
                    est.append({'code': option.get('value'), 'name': option.text.strip()})
             return est
        return []

    def get_case_types(self, state_code, dist_code, court_complex_code_full, est_code=''):
        parts = court_complex_code_full.split('@')
        complex_code = parts[0]
        
        data = {
            'state_code': state_code,
            'dist_code': dist_code,
            'court_complex_code': complex_code,
            'est_code': est_code,
            'search_type': 'c_no'
        }
        resp = self._post('casestatus/fillCaseType', data)
        if isinstance(resp, dict) and 'casetype_list' in resp:
             soup = BeautifulSoup(resp['casetype_list'], 'html.parser')
             types = []
             for option in soup.find_all('option'):
                 if option.get('value') and option.get('value') != '0':
                    types.append({'code': option.get('value'), 'name': option.text.strip()})
             return types
        return []

    def get_captcha_image(self):
        resp = self._post('casestatus/getCaptcha', {})
        if isinstance(resp, dict) and 'div_captcha' in resp:
            soup = BeautifulSoup(resp['div_captcha'], 'html.parser')
            img_tag = soup.find('img')
            if img_tag and img_tag.get('src'):
                img_src = img_tag.get('src')
                
                if img_src.startswith('/'):
                    base = "https://services.ecourts.gov.in"
                    captcha_url = f"{base}{img_src}"
                else:
                    captcha_url = f"{self.base_url}/{img_src}"

                if '?' in captcha_url:
                    captcha_url = f"{captcha_url}&app_token={self.app_token}"
                else:
                    captcha_url = f"{captcha_url}?app_token={self.app_token}"
                
                try:
                    img_resp = self.session.get(captcha_url, verify=False, timeout=10)
                    return img_resp.content
                except Exception as e:
                    logger.error(f"Failed to fetch captcha image: {e}")
        return None

    def search_by_case_no(self, state_code, dist_code, court_complex_code, case_type, case_no, year, est_code=''):
        """
        Searches for a case and returns data in standard format.
        """
        # Ensure session is initialized
        if not self.app_token:
            self.initialize_session()
            
        result = self.search_case(state_code, dist_code, court_complex_code, est_code, case_type, case_no, year)
        
        if result.get('status') == 'success':
            standard_cases = []
            for case in result.get('cases', []):
                # Filter out header rows or invalid rows
                # A valid case usually has details_params or at least a case_no/cino
                if not case.get('details_params') and not case.get('cino'):
                     continue

                # Map fields to standard format
                std_case = {
                    'cino': case.get('cino'),
                    'case_no': case.get('case_no'),
                    'pet_name': case.get('pet_name'),
                    'res_name': case.get('res_name'),
                    'status': '', # Not available in summary
                    'next_hearing_date': '', # Not available in summary
                    'details_params': case.get('details_params')
                }
                standard_cases.append(std_case)
            return standard_cases
        return []

    def _format_causelist_date(self, listing_date: str) -> str:
        value = (listing_date or "").strip()
        if not value:
            return value

        if re.fullmatch(r"\d{2}-\d{2}-\d{4}", value):
            return value

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            year, month, day = value.split("-")
            return f"{day}-{month}-{year}"

        return value

    def _extract_pdf_links_from_payload(self, payload: Any) -> List[str]:
        links: List[str] = []
        seen = set()

        def _add(link: str) -> None:
            if not link:
                return
            cleaned = html.unescape(link.strip())
            if not cleaned:
                return
            if not cleaned.lower().startswith(("http://", "https://")):
                cleaned = urljoin(f"{self.base_url}/", cleaned.lstrip("/"))
            cleaned = cleaned.replace("\\/", "/")
            if cleaned.lower().endswith(".pdf") and cleaned not in seen:
                seen.add(cleaned)
                links.append(cleaned)

        def _scan(value: Any) -> None:
            if isinstance(value, dict):
                for nested in value.values():
                    _scan(nested)
                return
            if isinstance(value, list):
                for nested in value:
                    _scan(nested)
                return
            if not isinstance(value, str):
                return

            for match in re.findall(r"(?:href|src)=[\"']([^\"']+\.pdf(?:\?[^\"']*)?)[\"']", value, flags=re.I):
                _add(match)

            for match in re.findall(r"https?://[^\s\"'>]+\.pdf(?:\?[^\s\"'>]+)?", value, flags=re.I):
                _add(match)

            for match in re.findall(r"[A-Za-z0-9_\-/\.]+\.pdf(?:\?[^\s\"'>]+)?", value, flags=re.I):
                if "/" in match:
                    _add(match)

        _scan(payload)
        return links

    def _download_pdf_bytes(self, pdf_url: str) -> bytes:
        response = self.session.get(pdf_url, verify=False, timeout=30)
        response.raise_for_status()
        return response.content

    def fetch_cause_list(
        self,
        state_code: str,
        dist_code: str,
        court_complex_code_full: str,
        listing_date: str,
        est_code: str = "",
        registration_no: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch District Court cause-list PDF(s) and parse case-number entries.
        If registration_no is provided, only matching entries are returned in
        `matched_entries` while `entries` contains all parsed case-number entries.
        """
        if not self.app_token and not self.initialize_session():
            return {"status": "error", "msg": "Failed to initialize session"}

        parts = (court_complex_code_full or "").split("@")
        complex_code = parts[0] if parts else ""
        formatted_date = self._format_causelist_date(listing_date)

        # Prime causelist page cookies/context.
        try:
            self.session.get(f"{self.base_url}/?p=clsearch/index", verify=False, timeout=30)
        except Exception:
            logger.debug("Unable to pre-load clsearch index", exc_info=True)

        payload_candidates = [
            {
                "state_code": state_code,
                "dist_code": dist_code,
                "court_complex_code": complex_code,
                "est_code": est_code,
                "listing_date": formatted_date,
            },
            {
                "state_code": state_code,
                "dist_code": dist_code,
                "court_complex_code": complex_code,
                "est_code": est_code,
                "cause_list_date": formatted_date,
            },
            {
                "state_code": state_code,
                "dist_code": dist_code,
                "court_complex_code": complex_code,
                "est_code": est_code,
                "date_from": formatted_date,
                "date_to": formatted_date,
            },
        ]
        endpoint_candidates = [
            "clsearch/submit",
            "clsearch/submitDate",
            "clsearch/getCauselist",
            "clsearch/showlist",
            "clsearch/display",
        ]

        response_payloads: List[Any] = []
        pdf_links: List[str] = []

        for endpoint in endpoint_candidates:
            if pdf_links:
                break
            for payload in payload_candidates:
                try:
                    resp = self._post(endpoint, payload.copy())
                    response_payloads.append(resp)
                    links = self._extract_pdf_links_from_payload(resp)
                    if links:
                        pdf_links.extend(links)
                        break
                except Exception:
                    logger.debug("Cause-list request failed for endpoint %s", endpoint, exc_info=True)

        # Fallback: scan clsearch page itself for direct PDF links.
        if not pdf_links:
            try:
                index_resp = self.session.get(
                    f"{self.base_url}/?p=clsearch/index",
                    verify=False,
                    timeout=30,
                )
                response_payloads.append(index_resp.text)
                pdf_links.extend(self._extract_pdf_links_from_payload(index_resp.text))
            except Exception:
                logger.debug("Failed to scan clsearch index for PDF links", exc_info=True)

        # Deduplicate while preserving order.
        deduped_links: List[str] = []
        seen_links = set()
        for link in pdf_links:
            if link not in seen_links:
                seen_links.add(link)
                deduped_links.append(link)

        pdf_results: List[Dict[str, Any]] = []
        all_entries: List[Dict[str, Any]] = []
        matched_entries: List[Dict[str, Any]] = []

        for pdf_url in deduped_links:
            try:
                pdf_bytes = self._download_pdf_bytes(pdf_url)
            except Exception as exc:
                pdf_results.append(
                    {
                        "url": pdf_url,
                        "status": "error",
                        "error": str(exc),
                        "entries": [],
                        "matched_entries": [],
                    }
                )
                continue

            tmp_path: Optional[str] = None
            try:
                with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
                    tmp_pdf.write(pdf_bytes)
                    tmp_path = tmp_pdf.name

                parsed_entries = parse_dc_cause_list_pdf(tmp_path)
                if registration_no:
                    parsed_matched_entries = find_dc_case_entries(tmp_path, registration_no)
                else:
                    parsed_matched_entries = parsed_entries

                pdf_results.append(
                    {
                        "url": pdf_url,
                        "status": "success",
                        "entries": parsed_entries,
                        "matched_entries": parsed_matched_entries,
                    }
                )
                all_entries.extend(parsed_entries)
                matched_entries.extend(parsed_matched_entries)
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)

        if not deduped_links:
            return {
                "status": "error",
                "msg": "Could not locate cause-list PDF links from District Court cause-list responses",
                "listing_date": formatted_date,
                "responses_checked": len(response_payloads),
                "pdfs": [],
                "entries": [],
                "matched_entries": [],
            }

        return {
            "status": "success",
            "listing_date": formatted_date,
            "pdfs": pdf_results,
            "entries": all_entries,
            "matched_entries": matched_entries,
        }

    def get_case_details(self, case_params):
        """
        Fetches detailed case info using params from view_action.
        """
        # We need to map the keys if they are different from what viewHistory expects
        # viewHistory params: court_code, state_code, dist_code, court_complex_code, case_no, cino, hideparty, search_flag, search_by
        
        if not case_params:
            return {'status': 'error', 'msg': 'No parameters provided'}

        resp = self._post('home/viewHistory', case_params)
        if isinstance(resp, dict) and 'data_list' in resp:
            return self._parse_case_details(resp['data_list'])
        return {'status': 'error', 'msg': 'Failed to fetch details'}

    def _fetch_pdf_url(self, params):
        """
        Fetches the actual temporary PDF path from the params by performing a POST request.
        """
        if not params:
            return None
            
        try:
            # The endpoint expects these keys: normal_v, case_val, court_code, filename, appFlag
            resp = self._post('home/display_pdf', params)
            
            if isinstance(resp, dict):
                # Some responses have 'status', others just have 'order'
                status = resp.get('status')
                order_path = resp.get('order')
                
                if (status is None and order_path) or \
                   (status == True or status == 1 or str(status).lower() == 'true'):
                     if order_path:
                         url = f"{self.base_url}/{order_path}"
                         if self.app_token:
                             if '?' in url:
                                 url = f"{url}&app_token={self.app_token}"
                             else:
                                 url = f"{url}?app_token={self.app_token}"
                         return url

        except Exception as e:
            logger.error(f"Failed to fetch PDF path: {e}")
            
        return None

    def _parse_case_details(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        details = {
            'cino': None,
            'case_no': None,
            'case_type': None,
            'filing_no': None,
            'filing_date': None,
            'registration_no': None,
            'registration_date': None,
            'first_hearing_date': None,
            'decision_date': None,
            'status': None,
            'nature_of_disposal': None,
            'court_no_judge': None,
            'pet_name': None,
            'res_name': None,
            'acts': [],
            'history': [],
            'orders': []
        }
        
        # Helper to find value in tables
        def get_table_value(table, label_text):
            if not table: return None
            label = table.find('label', string=re.compile(label_text, re.I)) or \
                    table.find(string=re.compile(label_text, re.I))
            if label:
                # Value is usually in the next td or same td
                # Try finding parent td then next sibling td
                td = label.find_parent('td')
                if td:
                    next_td = td.find_next_sibling('td')
                    if next_td:
                        return next_td.get_text(strip=True)
            return None

        # Case Details Table
        cd_table = soup.find('table', class_='case_details_table')
        if cd_table:
            details['case_type'] = get_table_value(cd_table, 'Case Type')
            details['filing_no'] = get_table_value(cd_table, 'Filing Number')
            details['filing_date'] = get_table_value(cd_table, 'Filing Date')
            details['registration_no'] = get_table_value(cd_table, 'Registration Number')
            details['registration_date'] = get_table_value(cd_table, 'Registration Date')
            
            # CNR Number is special
            cnr_span = cd_table.find('span', class_='text-danger')
            if cnr_span:
                details['cino'] = cnr_span.get_text(strip=True)

        # Case Status Table
        cs_table = soup.find('table', class_='case_status_table')
        if cs_table:
            details['first_hearing_date'] = get_table_value(cs_table, 'First Hearing Date')
            details['decision_date'] = get_table_value(cs_table, 'Decision Date')
            details['status'] = get_table_value(cs_table, 'Case Status')
            details['nature_of_disposal'] = get_table_value(cs_table, 'Nature of Disposal')
            details['court_no_judge'] = get_table_value(cs_table, 'Court Number and Judge')

        # Petitioner and Respondent
        pet_table = soup.find('table', class_='Petitioner_Advocate_table')
        if pet_table:
            details['pet_name'] = [pet_table.get_text(separator=' ', strip=True)]
            
        res_table = soup.find('table', class_='Respondent_Advocate_table')
        if res_table:
            details['res_name'] = [res_table.get_text(separator=' ', strip=True)]

        # Acts
        act_table = soup.find('table', id='act_table') or soup.find('table', class_='acts_table')
        if act_table:
            rows = act_table.find_all('tr')
            if len(rows) > 1:
                for row in rows[1:]:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        details['acts'].append({
                            'act': cols[0].get_text(strip=True),
                            'section': cols[1].get_text(strip=True)
                        })

        # History
        hist_table = soup.find('table', class_='history_table')
        if hist_table:
            rows = hist_table.find_all('tr')
            # Check header to confirm columns, but assuming standard: Judge, Business Date, Hearing Date, Purpose
            if len(rows) > 1:
                for row in rows[1:]:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        details['history'].append({
                            'judge': cols[0].get_text(strip=True),
                            'business_date': cols[1].get_text(strip=True),
                            'hearing_date': cols[2].get_text(strip=True),
                            'purpose': cols[3].get_text(strip=True)
                        })

        # Orders
        # There might be multiple order tables (Interim, Final)
        order_tables = soup.find_all('table', class_='order_table')
        for ot in order_tables:
            rows = ot.find_all('tr')
            if len(rows) > 1:
                for row in rows[1:]:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        order_no = cols[0].get_text(strip=True)
                        order_date = cols[1].get_text(strip=True)
                        order_details = cols[2].get_text(strip=True)
                        
                        document_url = None
                        pdf_params = None
                        
                        # Search in the raw HTML of the cell to handle malformed/nested onclicks
                        cell_html = str(cols[2])
                        
                        # Look for 4-5 quoted arguments: 'arg1', 'arg2', 'arg3', 'arg4' [, 'arg5']
                        # This regex is more robust against mangled HTML around the function call
                        param_matches = re.findall(r"['\"]([^'\"]*)['\"]\s*,\s*['\"]([^'\"]*)['\"]\s*,\s*['\"]([^'\"]*)['\"]\s*,\s*['\"]([^'\"]*)['\"](?:\s*,\s*['\"]([^'\"]*)['\"])?", cell_html)
                        
                        if param_matches:
                            # Take the last match as it's most likely the intended inner call in nested scenarios
                            args = param_matches[-1]
                            
                            pdf_params = {
                                'normal_v': args[0],
                                'case_val': args[1],
                                'court_code': args[2],
                                'filename': args[3],
                                'appFlag': args[4] if len(args) > 4 else ''
                            }
                            
                            # Fetch actual URL (requires POST to generate the temporary file)
                            # This is slower but necessary as the server requires POST to 'home/display_pdf'
                            document_url = self._fetch_pdf_url(pdf_params)
                        
                        details['orders'].append({
                            'order_no': order_no,
                            'date': order_date,
                            'description': order_details,
                            'document_url': document_url,
                            'pdf_params': pdf_params
                        })
                        
        return details

    def search_case(self, state_code, dist_code, court_complex_code_full, est_code, case_type, case_no, year):
        parts = court_complex_code_full.split('@')
        complex_code = parts[0]
        
        # Retry logic for CAPTCHA
        max_retries = 3
        for attempt in range(max_retries):
            captcha_img = self.get_captcha_image()
            if not captcha_img:
                logger.error("Failed to get captcha image")
                continue
            
            try:
                captcha_text = self.ocr.classification(captcha_img)
            except Exception as e:
                logger.error(f"OCR failed: {e}")
                continue

            # logger.info(f"Attempt {attempt+1}: Solved Captcha: {captcha_text}")
            
            data = {
                'search_case_no': case_no,
                'case_no': case_no,
                'rgyear': year,
                'case_captcha_code': captcha_text,
                'case_type': case_type,
                'state_code': state_code,
                'dist_code': dist_code,
                'court_complex_code': complex_code,
                'est_code': est_code,
                'submit_btn': 'Go'
            }
            
            resp = self._post('casestatus/submitCaseNo', data)
            
            if isinstance(resp, dict):
                if resp.get('status') == 1:
                    return self._parse_results(resp.get('case_data', ''))
                elif 'captcha' in str(resp.get('msg', '')).lower() or 'captcha' in str(resp.get('div_captcha', '')).lower():
                    # Captcha error, retry
                    logger.info("Captcha failed, retrying...")
                    continue
                else:
                    # Other error or no records
                    return {'status': 'error', 'msg': resp.get('msg', 'Unknown error'), 'raw': resp}
            else:
                 # Should not happen with _post returning dict or text
                 logger.error(f"Unexpected response type: {type(resp)}")
                 continue
                 
        return {'status': 'error', 'msg': 'Max retries exceeded or captcha failed'}

    def _parse_results(self, html_content):
        if 'Record not found' in html_content:
            return {'status': 'success', 'cases': []}
            
        soup = BeautifulSoup(html_content, 'html.parser')
        cases = []
        
        # Table structure is usually: headings... then rows.
        table = soup.find('table', {'id': 'search_res_table'}) or soup.find('table')
        if not table:
             return {'status': 'success', 'raw_html': html_content, 'cases': []}

        rows = table.find_all('tr')
        if not rows:
             return {'status': 'success', 'raw_html': html_content, 'cases': []}

        # Header detection
        headers = [th.text.strip() for th in rows[0].find_all(['th', 'td'])]
        
        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) < 2:
                continue
                
            case_info = {}
            for i, col in enumerate(cols):
                header = headers[i] if i < len(headers) else f"col_{i}"
                case_info[header] = col.text.strip()
                
                # Check for view link/button which might contain the CNR or case parameters
                view_link = col.find('a') or col.find('button')
                if view_link:
                    case_info['view_action'] = view_link.get('onclick') or view_link.get('href')

            # Extract standard fields
            # "Case Type/Case Number/Case Year" -> case_no
            # "Petitioner Name versus Respondent Name" -> pet_name, res_name
            
            raw_case_no = None
            raw_parties = None
            
            for k, v in case_info.items():
                if 'Case Number' in k:
                    raw_case_no = v
                if 'Petitioner' in k and 'Respondent' in k:
                    raw_parties = v
            
            if raw_case_no:
                case_info['case_no'] = raw_case_no
            
            if raw_parties:
                if 'Vs' in raw_parties:
                    parts = raw_parties.split('Vs')
                    case_info['pet_name'] = [parts[0].strip()]
                    case_info['res_name'] = [parts[1].strip()]
                elif 'versus' in raw_parties.lower():
                    # Handle "versus" or other separators if needed
                    parts = re.split(r'\s+versus\s+', raw_parties, flags=re.I)
                    if len(parts) >= 2:
                        case_info['pet_name'] = [parts[0].strip()]
                        case_info['res_name'] = [parts[1].strip()]
                    else:
                        case_info['pet_name'] = [raw_parties]
                        case_info['res_name'] = []

            # Parse view_action for params
            if case_info.get('view_action'):
                va = case_info['view_action']
                m = re.search(r"viewHistory\((.*)\)", va)
                if m:
                    args = [x.strip().strip("'") for x in m.group(1).split(',')]
                    if len(args) >= 9:
                        # case_no, cino, sel_court_code, hideparty, caseStatusSearchType, state_code, dist_code, complex_code, search_by
                        case_info['details_params'] = {
                            'case_no': args[0],
                            'cino': args[1],
                            'court_code': args[2],
                            'hideparty': args[3],
                            'search_flag': args[4],
                            'state_code': args[5],
                            'dist_code': args[6],
                            'court_complex_code': args[7],
                            'search_by': args[8]
                        }
                        case_info['cino'] = args[1]

            cases.append(case_info)
        
        return {'status': 'success', 'cases': cases}

async def persist_orders_to_storage(
    orders: List[Dict[str, Any]] | None,
    case_id: str | None = None,
    scraper: Optional["EcourtsWebScraper"] = None,
) -> List[Dict[str, Any]] | None:
    """
    Upload scraped eCourts order documents to storage and update their URLs.
    """
    def fetch_fn(url: str, referer: str | None = None) -> requests.Response:
        session = scraper.session if scraper else requests
        headers = {}
        if referer:
            headers["Referer"] = referer
        return session.get(url, headers=headers, verify=False, timeout=30)

    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=fetch_fn,
        referer="https://services.ecourts.gov.in/",
    )

if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description="Search eCourts cases by Case Number")
    parser.add_argument("--state", required=True, help="State Code")
    parser.add_argument("--district", required=True, help="District Code")
    parser.add_argument("--complex", required=True, help="Court Complex Code (full string with @)")
    parser.add_argument("--casetype", required=True, help="Case Type Code")
    parser.add_argument("--caseno", required=True, help="Case Number")
    parser.add_argument("--year", required=True, help="Registration Year")
    parser.add_argument("--est", default="", help="Establishment Code (if applicable)")
    
    args = parser.parse_args()
    
    scraper = EcourtsWebScraper()
    print(f"Searching for Case {args.caseno}/{args.year}...")
    
    # Initialize session first
    if scraper.initialize_session():
        result = scraper.search_case(
            args.state, 
            args.district, 
            args.complex, 
            args.est, 
            args.casetype, 
            args.caseno, 
            args.year
        )
        print("Result:", result)
    else:
        print("Failed to initialize session.")
