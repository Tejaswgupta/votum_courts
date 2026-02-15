import logging
import os
import re
from datetime import datetime, timedelta
from hashlib import sha256
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import fitz
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
CAUSE_LIST_URL = f"{BASE_URL}/web/cause-lists/cause-list"
CAUSE_LIST_ARCHIVE_URL = f"{BASE_URL}/web/cause-lists/cause-list-archive"

CAUSE_LIST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

DELHI_CASE_NO_PATTERN = re.compile(
    r"\b[A-Z][A-Z0-9()./&-]{0,40}\s*\d{1,7}/\d{4}\b"
)


def parse_listing_date(date_str: Optional[str]) -> datetime:
    if not date_str:
        return datetime.now() + timedelta(days=1)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError("Invalid listing_date format. Use YYYY-MM-DD, DD/MM/YYYY, or DD-MM-YYYY.")


def _normalize_case_token(case_no: str) -> str:
    token = re.sub(r"\s+", " ", (case_no or "").upper()).strip()
    token = token.replace(" /", "/").replace("/ ", "/")
    return token


def _case_tail(case_no: str) -> str:
    token = _normalize_case_token(case_no)
    match = re.search(r"(\d{1,7}/\d{4})", token)
    if match:
        return match.group(1)
    compact = re.sub(r"[^A-Z0-9/]+", "", token)
    parts = compact.split("/")
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return compact


def _clean_pdf_line(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if not cleaned:
        return ""
    if cleaned.startswith("Page ") and " of " in cleaned:
        return ""
    if cleaned.startswith("Created on "):
        return ""
    if cleaned in {"IT CELL", "GOTO TOP", "FIRST PAGE"}:
        return ""
    return cleaned


def _extract_case_tokens(lines: List[str]) -> List[str]:
    seen = set()
    values: List[str] = []
    for line in lines:
        for token in DELHI_CASE_NO_PATTERN.findall((line or "").upper()):
            normalized = _normalize_case_token(token)
            if normalized and normalized not in seen:
                seen.add(normalized)
                values.append(normalized)
    return values


def _parse_single_cause_list_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    raw_lines = entry.get("raw_lines") or []
    case_numbers = _extract_case_tokens(raw_lines)
    case_no = case_numbers[0] if case_numbers else None
    text = "\n".join(raw_lines).strip()
    entry_hash_src = f"{entry.get('item_no')}|{entry.get('page_no')}|{text}"
    entry_hash = sha256(entry_hash_src.encode("utf-8")).hexdigest()
    return {
        "item_no": entry.get("item_no"),
        "page_no": entry.get("page_no"),
        "case_no": case_no,
        "case_nos": case_numbers,
        "text": text,
        "entry_hash": entry_hash,
    }


def parse_cause_list_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Parse Delhi HC cause-list PDF and keep only case-number entries.
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
                    if y0 < 100 or y0 > 780:
                        continue
                    line_text = "".join(span.get("text", "") for span in line.get("spans", []))
                    cleaned = _clean_pdf_line(line_text)
                    if not cleaned:
                        continue
                    lines.append({"x": float(x0), "y": float(y0), "text": cleaned})

            lines.sort(key=lambda item: (item["y"], item["x"]))
            starts = [
                line
                for line in lines
                if line["x"] < 75 and re.fullmatch(r"\d{1,4}", line["text"])
            ]
            starts.sort(key=lambda item: item["y"])

            if not starts:
                if open_entry:
                    open_entry["raw_lines"].extend(line["text"] for line in lines)
                continue

            first_start_y = starts[0]["y"]
            if open_entry:
                for line in lines:
                    if line["y"] >= first_start_y:
                        continue
                    open_entry["raw_lines"].append(line["text"])
                entries.append(_parse_single_cause_list_entry(open_entry))
                open_entry = None

            for idx, start in enumerate(starts):
                y_start = start["y"]
                y_end = starts[idx + 1]["y"] if idx + 1 < len(starts) else float("inf")
                segment = {
                    "item_no": start["text"],
                    "page_no": page_idx + 1,
                    "raw_lines": [],
                }
                for line in lines:
                    if y_start <= line["y"] < y_end:
                        segment["raw_lines"].append(line["text"])

                if idx + 1 < len(starts):
                    entries.append(_parse_single_cause_list_entry(segment))
                else:
                    open_entry = segment

        if open_entry:
            entries.append(_parse_single_cause_list_entry(open_entry))

    return [entry for entry in entries if entry.get("case_nos")]


def find_case_entries(pdf_path: str, case_no: str) -> List[Dict[str, Any]]:
    """
    Find Delhi HC cause-list entries that match a case number.
    Matching is based on case tail: <number>/<year>.
    """
    target_tail = _case_tail(case_no)
    parsed = parse_cause_list_pdf(pdf_path)
    if not target_tail:
        return parsed

    matched_entries: List[Dict[str, Any]] = []
    for entry in parsed:
        case_nos = entry.get("case_nos") or []
        tails = {_case_tail(value) for value in case_nos if value}
        if target_tail in tails:
            matched_entries.append(entry)
    return matched_entries


def _extract_pdf_links_from_table(page_html: str, page_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(page_html, "html.parser")
    links: List[Dict[str, Any]] = []
    seen = set()

    for row in soup.find_all("tr"):
        title = ""
        listing_date = None
        cells = row.find_all("td")
        if len(cells) >= 3:
            title = cells[1].get_text(" ", strip=True)
            date_text = cells[2].get_text(" ", strip=True)
            date_match = re.search(r"\d{2}[-/]\d{2}[-/]\d{4}", date_text)
            if date_match:
                listing_date = date_match.group(0).replace("/", "-")

        for tag in row.find_all("a", href=True):
            href = (tag.get("href") or "").strip()
            if not href:
                continue
            lower_href = href.lower()
            if lower_href.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg")):
                continue
            if ".pdf" not in lower_href:
                continue
            pdf_url = urljoin(page_url, href)
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            links.append(
                {
                    "title": title or tag.get_text(" ", strip=True) or "Delhi HC Cause List",
                    "listing_date": listing_date,
                    "pdf_url": pdf_url,
                    "source_page": page_url,
                }
            )
    return links


def fetch_cause_list_pdfs(
    listing_date: datetime,
    include_archive: bool = True,
    max_pages: int = 3,
    title_contains: Optional[str] = "Cause List of Sitting of Benches",
) -> List[Dict[str, Any]]:
    """
    Discover Delhi HC cause-list PDFs for a specific date.
    """
    date_token = listing_date.strftime("%d-%m-%Y")
    all_page_urls: List[str] = []

    for page_idx in range(max_pages):
        suffix = f"?page={page_idx}" if page_idx else ""
        all_page_urls.append(f"{CAUSE_LIST_URL}{suffix}")
        if include_archive:
            all_page_urls.append(f"{CAUSE_LIST_ARCHIVE_URL}{suffix}")

    found: List[Dict[str, Any]] = []
    seen_urls = set()
    for page_url in all_page_urls:
        try:
            resp = requests.get(page_url, headers=CAUSE_LIST_HEADERS, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("Failed to fetch cause-list index page %s: %s", page_url, exc)
            continue

        for item in _extract_pdf_links_from_table(resp.text, page_url):
            if item["pdf_url"] in seen_urls:
                continue
            if item.get("listing_date") and item["listing_date"] != date_token:
                continue
            title = (item.get("title") or "").lower()
            if title_contains and title_contains.lower() not in title:
                continue
            found.append(item)
            seen_urls.add(item["pdf_url"])

    return found


def fetch_cause_list_pdf_bytes(
    listing_date: datetime,
    include_archive: bool = True,
    max_pages: int = 3,
    title_contains: Optional[str] = "Cause List of Sitting of Benches",
) -> bytes:
    """
    Fetch the first matching Delhi HC cause-list PDF bytes for the given date.
    """
    pdfs = fetch_cause_list_pdfs(
        listing_date=listing_date,
        include_archive=include_archive,
        max_pages=max_pages,
        title_contains=title_contains,
    )
    if not pdfs and title_contains:
        pdfs = fetch_cause_list_pdfs(
            listing_date=listing_date,
            include_archive=include_archive,
            max_pages=max_pages,
            title_contains=None,
        )
    if not pdfs:
        raise ValueError(f"No Delhi HC cause-list PDF found for {listing_date.strftime('%d-%m-%Y')}")

    response = requests.get(
        pdfs[0]["pdf_url"],
        headers=CAUSE_LIST_HEADERS,
        timeout=60,
    )
    response.raise_for_status()
    return response.content


def fetch_cause_list_entries(
    listing_date: Optional[str] = None,
    case_no: Optional[str] = None,
    include_archive: bool = True,
    max_pages: int = 3,
) -> Dict[str, Any]:
    """
    Fetch Delhi HC cause-list PDFs for date and parse case-number entries.
    If case_no is passed, matched_entries contains only matching rows.
    """
    target_date = parse_listing_date(listing_date)
    date_token = target_date.strftime("%d-%m-%Y")
    pdfs = fetch_cause_list_pdfs(
        listing_date=target_date,
        include_archive=include_archive,
        max_pages=max_pages,
    )

    pdf_results: List[Dict[str, Any]] = []
    all_entries: List[Dict[str, Any]] = []
    matched_entries: List[Dict[str, Any]] = []

    for pdf in pdfs:
        tmp_path: Optional[str] = None
        try:
            resp = requests.get(pdf["pdf_url"], headers=CAUSE_LIST_HEADERS, timeout=60)
            resp.raise_for_status()
            with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
                tmp_pdf.write(resp.content)
                tmp_path = tmp_pdf.name

            parsed_entries = parse_cause_list_pdf(tmp_path)
            case_matched_entries = (
                find_case_entries(tmp_path, case_no) if case_no else parsed_entries
            )
            all_entries.extend(parsed_entries)
            matched_entries.extend(case_matched_entries)
            pdf_results.append(
                {
                    "url": pdf["pdf_url"],
                    "title": pdf.get("title"),
                    "status": "success",
                    "entries": parsed_entries,
                    "matched_entries": case_matched_entries,
                }
            )
        except Exception as exc:
            pdf_results.append(
                {
                    "url": pdf["pdf_url"],
                    "title": pdf.get("title"),
                    "status": "error",
                    "error": str(exc),
                    "entries": [],
                    "matched_entries": [],
                }
            )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    return {
        "status": "success" if pdf_results else "error",
        "listing_date": date_token,
        "pdfs": pdf_results,
        "entries": all_entries,
        "matched_entries": matched_entries,
        "searched_case_no": case_no,
    }

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
                    case_info['next_listing_date'] = date_match.group(1)
                
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



