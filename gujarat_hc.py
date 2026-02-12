import hashlib
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import ddddocr
import fitz
import httpx
import requests
from cron_jobs.task_email import send_smtp_email
from cron_jobs.task_notifications import send_fcm_notification
from cron_jobs.task_sms import send_sms_message
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
VOTUM_NOTIFICATIONS_TABLE = "votum_notifications"

BASE_URL = "https://gujarathc-casestatus.nic.in/gujarathc"
CASE_TYPE_URL = f"{BASE_URL}/GetCaseTypeDataOnLoad"
CAPTCHA_URL = f"{BASE_URL}/CaptchaServlet?ct=S&tm={{}}"
DATA_URL = f"{BASE_URL}/GetData"

# Cause List Constants
CAUSE_LIST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "en-GB,en;q=0.5",
    "cache-control": "no-cache",
    "content-type": "application/x-www-form-urlencoded",
    "pragma": "no-cache",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "sec-gpc": "1",
    "upgrade-insecure-requests": "1",
    "Referer": "https://gujarathc-casestatus.nic.in/gujarathc/",
}

CAUSE_LIST_HOME_URL = "https://gujarathc-casestatus.nic.in/gujarathc/"
CAUSE_LIST_PRINT_URL = "https://gujarathc-casestatus.nic.in/gujarathc/printBoardNew"

CASE_NO_PATTERN = re.compile(r"\b(?:[A-Z]{1,4}/)?[A-Z]{1,10}/\d{1,7}/\d{4}\b")


def _normalize_case_token(case_no: str) -> str:
    return re.sub(r"\s+", "", (case_no or "").upper())


def _case_tail(case_no: str) -> str:
    token = _normalize_case_token(case_no)
    parts = token.split("/")
    if len(parts) >= 3:
        return "/".join(parts[-3:])
    return token


def _is_vs_line(text: str) -> bool:
    normalized = re.sub(r"\s+", "", (text or "").upper())
    return normalized in {"V/S", "VS", "V.S", "V/S."}


def _clean_pdf_line(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if not cleaned:
        return ""
    if cleaned.startswith("Page ") and " of " in cleaned:
        return ""
    if cleaned in {"IT CELL", "GOTO TOP", "FIRST PAGE"}:
        return ""
    if cleaned.startswith("Created on "):
        return ""
    return cleaned


def _is_party_noise_line(text: str) -> bool:
    upper = (text or "").upper()
    if not upper:
        return True
    if upper in {
        "SNO",
        "CASE DETAILS",
        "NAME OF PARTIES",
        "NAME OF ADVOCATES",
        "REMARKS",
        "FRESH MATTERS",
    }:
        return True
    noise_markers = [
        "GOVERNMENT PLEADER",
        "ADVOCATE",
        "LAW ASSOCIATES",
        "SINGHI & CO",
        "LIST DATE:",
        "CORAM:",
        "COURT:",
        "PAGE ",
    ]
    if any(marker in upper for marker in noise_markers):
        return True
    if re.match(r"^(MR|MRS|MS|SMT|SHRI)\b", upper):
        return True
    return False


def _parse_single_cause_list_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    case_lines = entry.get("case_lines") or []
    party_lines = entry.get("party_lines") or []
    raw_lines = entry.get("raw_lines") or []

    case_numbers: List[str] = []
    seen = set()
    for line in case_lines:
        for token in CASE_NO_PATTERN.findall(line):
            normalized = _normalize_case_token(token)
            if normalized and normalized not in seen:
                seen.add(normalized)
                case_numbers.append(normalized)

    petitioner: Optional[str] = None
    respondent: Optional[str] = None
    vs_indexes = [idx for idx, line in enumerate(party_lines) if _is_vs_line(line)]
    if vs_indexes:
        first_vs = vs_indexes[0]
        next_vs = vs_indexes[1] if len(vs_indexes) > 1 else len(party_lines)
        petitioner_lines = [
            line
            for line in party_lines[:first_vs]
            if line and not _is_vs_line(line) and not _is_party_noise_line(line)
        ]
        respondent_lines: List[str] = []
        petitioner_norm_set = {
            re.sub(r"[^A-Z0-9]+", "", line.upper()) for line in petitioner_lines if line
        }
        for line in party_lines[first_vs + 1:next_vs]:
            if not line or _is_vs_line(line) or _is_party_noise_line(line):
                continue
            norm_line = re.sub(r"[^A-Z0-9]+", "", line.upper())
            if norm_line and norm_line in petitioner_norm_set:
                break
            respondent_lines.append(line)
        petitioner = " ".join(petitioner_lines).strip() or None
        respondent = " ".join(respondent_lines).strip() or None

    case_no = case_numbers[0] if case_numbers else None
    party_names = None
    if petitioner and respondent:
        party_names = f"{petitioner} V/S {respondent}"
    elif petitioner:
        party_names = petitioner
    elif respondent:
        party_names = respondent

    text = "\n".join(raw_lines).strip()
    entry_hash_src = f"{entry.get('item_no')}|{entry.get('page_no')}|{text}"
    entry_hash = hashlib.sha256(entry_hash_src.encode("utf-8")).hexdigest()

    return {
        "item_no": entry.get("item_no"),
        "page_no": entry.get("page_no"),
        "case_no": case_no,
        "case_nos": case_numbers,
        "petitioner": petitioner,
        "respondent": respondent,
        "party_names": party_names,
        "text": text,
        "entry_hash": entry_hash,
    }


def parse_cause_list_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Parse Gujarat HC cause-list PDF and extract structured entries.

    Returns list of entries with:
    - item_no
    - case_no (first detected case number in CASE DETAILS column)
    - case_nos (all detected case numbers in CASE DETAILS column)
    - petitioner / respondent / party_names
    - page_no
    - text (raw merged entry text)
    - entry_hash
    """
    entries: List[Dict[str, Any]] = []

    with fitz.open(pdf_path) as doc:
        open_entry: Optional[Dict[str, Any]] = None

        for page_idx in range(doc.page_count):
            page = doc[page_idx]
            lines: List[Dict[str, Any]] = []

            for block in page.get_text("dict").get("blocks", []):
                if block.get("type") != 0:
                    continue
                for line in block.get("lines", []):
                    x0, y0, _, _ = line.get("bbox", (0.0, 0.0, 0.0, 0.0))
                    if y0 < 140 or y0 > 770:
                        continue
                    line_text = "".join(span.get("text", "") for span in line.get("spans", []))
                    cleaned = _clean_pdf_line(line_text)
                    if not cleaned:
                        continue
                    lines.append({"x": float(x0), "y": float(y0), "text": cleaned})

            lines.sort(key=lambda item: (item["y"], item["x"]))
            page_tokens = {line["text"].upper() for line in lines}
            has_table_header = (
                "SNO" in page_tokens
                and "CASE DETAILS" in page_tokens
                and "NAME OF PARTIES" in page_tokens
            )

            if not has_table_header and not open_entry:
                continue

            starts = [
                line for line in lines if line["x"] < 70 and re.fullmatch(r"\d{1,4}", line["text"])
            ]
            starts.sort(key=lambda item: item["y"])

            if not starts:
                if open_entry:
                    for line in lines:
                        x = line["x"]
                        txt = line["text"]
                        open_entry["raw_lines"].append(txt)
                        if 70 <= x < 200:
                            open_entry["case_lines"].append(txt)
                        elif 200 <= x < 345:
                            open_entry["party_lines"].append(txt)
                continue

            first_start_y = starts[0]["y"]
            if open_entry:
                for line in lines:
                    if line["y"] >= first_start_y:
                        continue
                    x = line["x"]
                    txt = line["text"]
                    open_entry["raw_lines"].append(txt)
                    if 70 <= x < 200:
                        open_entry["case_lines"].append(txt)
                    elif 200 <= x < 345:
                        open_entry["party_lines"].append(txt)
                entries.append(_parse_single_cause_list_entry(open_entry))
                open_entry = None

            for idx, start in enumerate(starts):
                y_start = start["y"]
                y_end = starts[idx + 1]["y"] if idx + 1 < len(starts) else float("inf")
                segment = {
                    "item_no": start["text"],
                    "page_no": page_idx + 1,
                    "raw_lines": [],
                    "case_lines": [],
                    "party_lines": [],
                }
                for line in lines:
                    if not (y_start <= line["y"] < y_end):
                        continue
                    x = line["x"]
                    txt = line["text"]
                    segment["raw_lines"].append(txt)
                    if 70 <= x < 200:
                        segment["case_lines"].append(txt)
                    elif 200 <= x < 345:
                        segment["party_lines"].append(txt)

                if idx + 1 < len(starts):
                    entries.append(_parse_single_cause_list_entry(segment))
                else:
                    open_entry = segment

        if open_entry:
            entries.append(_parse_single_cause_list_entry(open_entry))

    return [entry for entry in entries if entry.get("case_nos")]


def find_case_entries(pdf_path: str, registration_no: str) -> List[Dict[str, Any]]:
    """
    Find cause-list entries that match a registration/case number.
    Supports matching both prefixed and non-prefixed forms:
    e.g. R/SCA/4937/2022 <-> SCA/4937/2022.
    """
    target_tail = _case_tail(registration_no)
    parsed = parse_cause_list_pdf(pdf_path)
    if not target_tail:
        return parsed

    matched_entries: List[Dict[str, Any]] = []
    for entry in parsed:
        case_nos = entry.get("case_nos") or []
        tails = {_case_tail(case_no) for case_no in case_nos if case_no}
        if target_tail in tails:
            matched_entries.append(entry)
    return matched_entries

def parse_listing_date(date_str: Optional[str]) -> datetime:
    if not date_str:
        return datetime.now() + timedelta(days=1)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError("Invalid listing_date format. Use YYYY-MM-DD or DD/MM/YYYY.")

def fetch_cause_list_pdf_bytes(listing_date: datetime) -> bytes:
    listing_date_str = listing_date.strftime("%d/%m/%Y")
    payload = {
        "coram": "",
        "coramOrder": "",
        "sidecode": "",
        "listflag": "5",
        "courtcode": "0",
        "courtroom": "undefined-undefined-undefined",
        "listingdate": listing_date_str,
        "advocatecodeval": "",
        "advocatenameval": "",
        "ccinval": "",
        "download_token": "",
    }

    with httpx.Client(
        headers=CAUSE_LIST_HEADERS, follow_redirects=True, timeout=60.0, verify=False
    ) as client:
        home_response = client.get(CAUSE_LIST_HOME_URL)
        home_response.raise_for_status()
        token_match = re.search(
            r'name="download_token"\s+value="([^"]*)"', home_response.text
        )
        if token_match:
            payload["download_token"] = token_match.group(1)

        response = client.post(CAUSE_LIST_PRINT_URL, data=payload)
        response.raise_for_status()
        return response.content


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
        value = date_str.strip()

        formats = [
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%d/%m/%y",
            "%d-%m-%y",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d.%m.%Y",
            "%d %b %Y",
            "%d %B %Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        match = re.search(r"(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})", value)
        if not match:
            return None

        day, month, year = match.groups()
        year = f"20{year}" if len(year) == 2 else year
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _get_section_records(
        self, data: List[Dict[str, Any]], section_key: str
    ) -> List[Dict[str, Any]]:
        """
        Gujarat HC response array ordering can vary between requests/cases.
        Find records by section key instead of hardcoded indexes.
        """
        for section in data:
            if not isinstance(section, dict):
                continue
            records = section.get(section_key)
            if isinstance(records, list):
                return records
        return []

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
            "connected_matters": [],
            "application_appeal_matters": [],
            "ia_details": [],
            "original_json": {
                "documents": [],
                "objections": [],
                "ia_details": [],
            },
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

        # Court proceedings (hearing history).
        linked_matters = self._get_section_records(data, "linkedmatterscp")
        for item in linked_matters:
            result["history"].append(
                {
                    "business_date": self._parse_date(item.get("PROCEEDINGDATElmcp")),
                    "hearing_date": self._parse_date(item.get("PROCEEDINGDATElmcp")),
                    "judge": item.get("JUDGESlmcp"),
                    "purpose": item.get("STAGENAMElmcp"),
                    "result": item.get("ACTIONNAMElmcp"),
                }
            )

        # Connected matters.
        for item in self._get_section_records(data, "linkedmatters"):
            result["connected_matters"].append(
                {
                    "case_no": item.get("casedescriptionlm"),
                    "cin_no": item.get("cinolm"),
                    "status": item.get("statusnamelm"),
                    "disposal_date": self._parse_date(item.get("disposaldatelm")),
                    "judge": item.get("JUDGESlm"),
                    "action": item.get("actionname"),
                }
            )

        # Application / Appeal matters.
        for item in self._get_section_records(data, "lpamatters"):
            result["application_appeal_matters"].append(
                {
                    "case_no": item.get("casedescriptionlm"),
                    "cin_no": item.get("cinolm"),
                    "status": item.get("statusnamelm"),
                    "judge": item.get("JUDGESlm"),
                    "disposal_date": self._parse_date(item.get("disposaldatelm")),
                    "action": item.get("actionname"),
                }
            )

        # IA Details (summary from main case API response).
        for item in self._get_section_records(data, "applicationmatters"):
            ia_number = (
                item.get("aino")
                or item.get("ia_no")
                or item.get("ia_number")
                or item.get("IANO")
                or item.get("iaNo")
            )
            description = (
                item.get("descriptionlm")
                or item.get("description")
                or item.get("ia_description")
                or item.get("DESC")
            )
            status = (
                item.get("statusnamelm")
                or item.get("status")
                or item.get("STATUS")
            )
            filing_date = self._parse_date(
                item.get("filingdatelm")
                or item.get("filing_date")
                or item.get("iafilingdate")
                or item.get("iadate")
            )
            next_date = self._parse_date(
                item.get("nextdatelm")
                or item.get("next_date")
                or item.get("nexthearingdate")
            )
            disposal_date = self._parse_date(
                item.get("disposaldatelm")
                or item.get("disposal_date")
            )
            party = (
                item.get("partyname")
                or item.get("litigantname")
                or item.get("party")
            )
            cin_no = item.get("ccin") or item.get("cin_no")

            if not any(
                [ia_number, description, status, filing_date, next_date, disposal_date, party, cin_no]
            ):
                continue

            result["ia_details"].append(
                {
                    "ia_no": ia_number,
                    "ia_number": ia_number,
                    "description": description,
                    "party": party,
                    "filing_date": filing_date,
                    "next_date": next_date,
                    "status": status,
                    "disposal_date": disposal_date,
                    "cin_no": cin_no,
                }
            )

        result["original_json"]["ia_details"] = result["ia_details"]

        # Tagged orders often represent linked tagging context.
        for item in self._get_section_records(data, "taggedorder"):
            result["connected_matters"].append(
                {
                    "source": "taggedorder",
                    "main_case_no": item.get("MAINCASE"),
                    "tagged_case_no": item.get("TAGCASE"),
                    "main_cin_no": item.get("mccin"),
                    "main_order_no": item.get("mno"),
                    "main_order_date": self._parse_date(item.get("mdate")),
                    "tagged_order_no": item.get("tno"),
                    "tagged_order_date": self._parse_date(item.get("tdate")),
                }
            )

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



@router.post("/cause_list/sync")
async def sync_cause_list(
    listing_date: Optional[str] = Form(None),
    dry_run: bool = Form(False),
    limit: Optional[int] = Form(None),
):
    """
    Fetch the next day's cause list, extract matching case rows by registration number,
    store extracted text + image path in 'cause_list_entries' table,
    and generate compiled PDFs per workspace.
    """
    supabase = get_supabase_client()
    run_id = _create_cron_job_run(
        supabase,
        "cause_list_sync",
        {"listing_date": listing_date, "dry_run": dry_run, "limit": limit},
    )
    run_status = "failed"
    run_error: str | None = None
    run_summary: Dict[str, Any] | None = None
    pdf_path: str | None = None

    try:
        try:
            target_date = parse_listing_date(listing_date)
        except ValueError as e:
            run_error = str(e)
            raise HTTPException(status_code=400, detail=str(e))

        listing_date_token = target_date.strftime("%Y%m%d")
        listing_date_db = target_date.strftime("%Y-%m-%d")

        try:
            pdf_bytes = fetch_cause_list_pdf_bytes(target_date)
        except Exception as e:
            run_error = f"Failed to fetch cause list PDF: {str(e)}"
            raise HTTPException(
                status_code=502,
                detail=f"Failed to fetch cause list PDF: {str(e)}",
            )

        with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            tmp_pdf.write(pdf_bytes)
            pdf_path = tmp_pdf.name

        storage_bucket = "documents"

        case_query = (
            supabase.table("votum_cases")
            .select(
                "id, workspace_id, registration_no, case_no, cin_no, "
                "assigned_user_ids, reminder_contacts, court_name"
            )
            .execute()
        )
        cases = case_query.data or []
        if limit:
            cases = cases[:limit]

        totals = {
            "total_cases": len(cases),
            "matched_cases": 0,
            "entries_added": 0,
            "images_uploaded": 0,
            "entries_updated": 0,
            "pdfs_generated": 0,
        }
        case_results = []
        
        # Accumulate entries for PDF generation: workspace_id -> list of entries
        workspace_entries: Dict[str, List[Dict[str, Any]]] = {}

        with TemporaryDirectory() as tmp_dir:
            for case in cases:
                registration_no = case.get("registration_no")
                if not registration_no:
                    continue

                # entries = find_case_entries(pdf_path, registration_no)
                # if not entries:
                #     continue

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

if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(get_gujarat_case_details("21", "4937", "2022")))
    # Print cause list entries for a case
    res = find_case_entries("/Users/tejaswgupta/Downloads/votum/backend/ecourts/Complete_Causelist_9th_February_2026.pdf", "SCA/4937/2022")
    print(res[0].get("text") if res else "No entries found")
