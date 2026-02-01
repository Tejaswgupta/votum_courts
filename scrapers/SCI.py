import ast
import asyncio
import logging
import operator
import random
import re
import string
from typing import Any, Dict, List, Optional

import ddddocr
import fitz
import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

try:
    from ..order_storage import persist_orders_to_storage as _persist_orders_to_storage
except ImportError:
    import sys
    from pathlib import Path
    # Add parent directory to path to allow importing order_storage
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from order_storage import persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TOKEN_URL = "https://www.sci.gov.in/case-status-party-name/"
CAPTCHA_URL = "https://www.sci.gov.in/?_siwp_captcha&id="
DATA_URL = "https://www.sci.gov.in/wp-admin/admin-ajax.php"

ocr = ddddocr.DdddOcr(show_ad=False)
session = requests.Session()
session.verify = False
session.cookies["PHPSESSID"] = "a0j600oa13tj40rupucvovgetq"
BASE_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Referer': 'https://www.sci.gov.in/case-status-party-name/',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=1',
    'TE': 'trailers'
}

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/123.0.0.0 Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0'
]

session.headers.update(BASE_HEADERS)


def _randomize_user_agent() -> None:
    session.headers['User-Agent'] = random.choice(USER_AGENTS)


def _session_get(url: str, **kwargs):
    _randomize_user_agent()
    return session.get(url, **kwargs)


_randomize_user_agent()


def validate_response(response):
    """Validate the response for successful status and expected structure."""
    if response.status_code != 200:
        logger.error(f"Unexpected status code: {response.status_code}")
        response.raise_for_status()
    
    try:
        return response.json()
    except ValueError as e:
        logger.error(f"Failed to parse JSON: {e}")
        raise


def _extract_html_fragment(payload: dict[str, Any]) -> Optional[str]:
    """Return HTML fragment from SCI payload or None when not available."""
    html_fragment = payload.get("data") if isinstance(payload, dict) else None

    if not html_fragment:
        logger.warning("SCI response did not include an HTML fragment: %s", payload)
        return None

    if isinstance(html_fragment, dict):
        message = html_fragment.get("message") if isinstance(html_fragment, dict) else html_fragment
        logger.warning(
            "SCI response returned a message instead of HTML: %s",
            message,
        )
        return None

    if isinstance(html_fragment, (list, tuple)):
        html_fragment = "".join(str(item) for item in html_fragment)

    if isinstance(html_fragment, bytes):
        html_fragment = html_fragment.decode("utf-8", errors="ignore")

    if not isinstance(html_fragment, str):
        logger.warning(
            "SCI response included unsupported data type %s for HTML fragment",
            type(html_fragment),
        )
        return None

    return html_fragment


def _normalize_captcha_expression(question: str) -> str:
    """Normalize OCR output into an arithmetic expression we can safely evaluate."""
    expr = question.strip()
    expr = expr.replace(" ", "")
    expr = expr.replace("=", "")
    expr = expr.replace("?", "")
    expr = expr.replace("×", "*")
    expr = expr.replace("x", "*")
    expr = expr.replace("X", "*")
    expr = expr.replace("÷", "/")
    expr = expr.replace(":", "/")
    expr = expr.replace("–", "-")
    expr = expr.replace("−", "-")

    allowed_chars = set("0123456789+-*/()")
    if not expr or any(ch not in allowed_chars for ch in expr):
        raise ValueError(f"Unsupported captcha expression: {question}")

    return expr


def _evaluate_captcha(question: str) -> int:
    """Safely evaluate a simple arithmetic captcha expression."""
    expr = _normalize_captcha_expression(question)

    allowed_operators = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
    }

    def _eval(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.BinOp):
            op_type = type(node.op)
            if op_type not in allowed_operators:
                raise ValueError(f"Unsupported operator in captcha: {op_type}")
            return allowed_operators[op_type](_eval(node.left), _eval(node.right))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = _eval(node.operand)
            return value if isinstance(node.op, ast.UAdd) else -value
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.Num):  # pragma: no cover  # for <3.8 compatibility
            return node.n
        raise ValueError(f"Unsupported captcha node: {ast.dump(node)}")

    result = _eval(ast.parse(expr, mode="eval"))

    if isinstance(result, int):
        return result
    if isinstance(result, float) and result.is_integer():
        return int(result)

    raise ValueError(f"Captcha did not resolve to an integer value: {result}")


def generate_random_string(length):
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))


def init_captcha(max_attempts: int = 3):
    """Fetch and solve the SCI captcha, retrying when OCR output is unusable."""

    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        token_page = _session_get(TOKEN_URL)
        token_page_text = token_page.text
        # print('='*20)
        # print(token_page_text)
        # print('='*20)
        captcha_id = token_page_text.split('name="scid" value="')[1].split('"')[0]
        captcha_image = _session_get(CAPTCHA_URL + captcha_id).content
        question = ocr.classification(captcha_image)

        try:
            answer = _evaluate_captcha(question)
        except ValueError as exc:  # pragma: no cover - depends on OCR output
            last_error = exc
            logger.warning(
                "Unable to parse captcha question '%s' on attempt %d/%d", question, attempt, max_attempts
            )
            continue

        token_name = token_page_text.split('<input type="hidden" id="tok_')[1].split('"')[0]
        token_value = (
            token_page_text.split('<input type="hidden" id="tok_')[1]
            .split('value="')[1]
            .split('"')[0]
        )
        return token_name, token_value, captcha_id, question, answer

    logger.error("Failed to solve SCI captcha after %d attempts", max_attempts)
    if last_error is not None:
        raise requests.exceptions.RequestException("Unable to solve SCI captcha") from last_error
    raise requests.exceptions.RequestException("Unable to solve SCI captcha")




def table_to_list(soup):
    columns = []
    for col in soup.find_all("th"):
        columns.append(col.text.strip())

    rows = soup.find_all("tr")[1:]

    results = []
    for row in rows:
        cells = row.find_all("td")

        if len(cells) < len(columns):
            continue

        row_data = [cell.text.strip() for cell in cells]

        data = dict(zip(columns, row_data))

        results.append(
            {
                "cino": data.get("Diary Number", ""),
                "date_of_decision": None,
                "pet_name": data.get("Petitioner Name", ""),
                "res_name": data.get("Respondent Name", ""),
                "type_name": data.get("Status", ""),
            }
        )

    return results


def _extract_td_text(
    soup: BeautifulSoup, label: str, separator: str = " "
) -> Optional[str]:
    """Return stripped text of the <td> following a label when present."""
    label_node = soup.find(string=lambda text: text and label in text)
    if not label_node:
        return None
    cell = label_node.find_next("td") if hasattr(label_node, "find_next") else None
    if not cell:
        return None
    return cell.get_text(separator=separator, strip=True)


def _fetch_case_tab(
    diary_no: str, diary_year: str, tab_name: str
) -> tuple[Optional[str], dict[str, Any]]:
    """Fetch a specific SCI case tab and return the raw HTML fragment."""
    data = {
        "diary_no": diary_no,
        "diary_year": diary_year,
        "tab_name": tab_name,
        "action": "get_case_details",
        "es_ajax_request": "1",
    }
    resp = _session_get(DATA_URL, params=data)
    payload = validate_response(resp)

    if not payload.get("success"):
        logger.warning("SCI tab %s responded with success=False", tab_name)
        return None, payload

    html_fragment = _extract_html_fragment(payload)
    return html_fragment, payload


def _normalize_key(label: str) -> str:
    """Convert column labels into snake_case keys."""
    normalized = re.sub(r"[^0-9a-z]+", "_", label.lower())
    normalized = re.sub(r"_{2,}", "_", normalized)
    return normalized.strip("_")


def _parse_listing_dates(html_fragment: str) -> list[dict[str, Any]]:
    """Parse listing dates table into a list of row dictionaries."""
    soup = BeautifulSoup(html_fragment, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    header_row = None
    if table.thead:
        header_row = table.thead.find("tr")
    elif table.find("tr"):
        header_row = table.find("tr")

    headers: list[str] = []
    if header_row:
        headers = [cell.get_text(" ", strip=True) for cell in header_row.find_all(["th", "td"])]

    body_rows = table.tbody.find_all("tr") if table.tbody else table.find_all("tr")
    if header_row and body_rows and body_rows[0] is header_row:
        body_rows = body_rows[1:]

    results: list[dict[str, Any]] = []
    for row in body_rows:
        cells = row.find_all("td")
        if not cells:
            continue

        row_data: dict[str, Any] = {}
        for idx, cell in enumerate(cells):
            header = headers[idx] if idx < len(headers) else f"column_{idx + 1}"
            key = _normalize_key(header) or f"column_{idx + 1}"
            text_value = cell.get_text(" ", strip=True)
            row_data[key] = text_value

            link = cell.find("a")
            if link and link.get("href"):
                row_data[f"{key}_url"] = link.get("href")

        results.append(row_data)

    return results


def _parse_judgement_orders(html_fragment: str) -> list[dict[str, Any]]:
    """Extract judgment/order documents with date and description."""
    soup = BeautifulSoup(html_fragment, "html.parser")
    rows: list[dict[str, Any]] = []

    for cell in soup.find_all("td"):
        link = cell.find("a")
        if not link or not link.get("href"):
            continue

        date_text = link.get_text(" ", strip=True)
        full_text = cell.get_text(" ", strip=True)
        trailing = full_text[len(date_text) :].strip()
        trailing = trailing.replace("\xa0", " ").strip()
        if trailing.startswith("[") and trailing.endswith("]"):
            trailing = trailing[1:-1].strip()

        rows.append(
            {
                "date": date_text,
                "description": trailing or None,
                "document_url": link.get("href"),
            }
        )

    return rows


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_diary_number(diary_number, diary_year):
    token_name, token_value, captcha_id, question, answer = init_captcha()

    data = {
        "action": "get_case_status_diary_no",
        "diary_no": diary_number,
        "year": diary_year,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "language": "en",
    }

    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)

        if response.get("success"):
            html_fragment = _extract_html_fragment(response)
            if not html_fragment:
                return []

            soup = BeautifulSoup(html_fragment, "html.parser")
            return table_to_list(soup)
        else:
            print("Request was unsuccessful.")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_case_number(case_type, case_number, case_year):
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "action": "get_case_status_case_no",
        "case_type": case_type,
        "case_no": case_number,
        "year": case_year,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "language": "en",
    }
    try:
        resp = _session_get(DATA_URL, params=data)
        # print(resp.text)
        response = validate_response(resp)
        if not response["success"]:
            if "captcha" in response.get("data", "").lower():
                #call the function again to retry
                return sci_search_by_case_number(case_type, case_number, case_year)
            return []

        html_fragment = response.get("data", {}).get('resultsHtml',"")
        if not html_fragment:
            return []
        soup = BeautifulSoup(html_fragment, "html.parser")
        

        return table_to_list(soup)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_aor_code(party_type, aor_code, year, case_status):
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "action": "get_case_status_aor_code",
        "party_type": party_type,
        "aor_code": aor_code,
        "year": year,
        "case_status": case_status,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "language": "en",
    }
    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)
        if response["success"]:
            html_fragment = _extract_html_fragment(response)
            if not html_fragment:
                return []

            soup = BeautifulSoup(html_fragment, "html.parser")
            return table_to_list(soup)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_party_name(party_type, party_name, year, party_status):
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "party_type": party_type,
        "party_name": party_name,
        "year": year,
        "party_status": party_status,
        "scid": captcha_id,
        "tok_" + token_name: token_value,
        "siwp_captcha_value": answer,
        "es_ajax_request": "1",
        "submit": "Search",
        "action": "get_case_status_party_name",
        "language": "en",
    }
    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)
        if response["success"]:
            html_fragment = _extract_html_fragment(response)
            if not html_fragment:
                return []

            soup = BeautifulSoup(html_fragment, "html.parser")
            return table_to_list(soup)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_search_by_court(
    court, state, bench, case_type, case_number, case_year, order_date
):
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "action": "get_case_status_court",
        "case_status_court": court,
        "case_status_state": state,
        "case_status_bench": bench,
        "case_status_case_type": case_type,
        "case_no": case_number,
        "year": case_year,
        "listing_date": order_date,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
        "language": "en",
    }
    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)
        if response["success"]:
            html_fragment = _extract_html_fragment(response)
            if not html_fragment:
                return []

            soup = BeautifulSoup(html_fragment, "html.parser")

            return table_to_list(soup)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_get_details(diary_no, diary_year):
    try:
        case_html, case_payload = _fetch_case_tab(diary_no, diary_year, "case_details")

        if not case_html:
            return None

        soup = BeautifulSoup(case_html, "html.parser")
        registration_no = (
            soup.find("h3").text.split("-")[1].strip() if soup.find("h3") else None
        )
        case_title = soup.find("h4").text.strip() if soup.find("h4") else None

        diary_info = _extract_td_text(soup, "Diary Number")
        diary_number = diary_info.split("Filed on", 1)[0].strip() if diary_info else None
        filing_date = None
        if diary_info:
            filing_parts = diary_info.split("Filed on", 1)
            if len(filing_parts) > 1:
                filing_date = filing_parts[1].split("[", 1)[0].strip()

        case_info = _extract_td_text(soup, "Case Number")
        case_number = (
            case_info.split("Registered", 1)[0].strip() if case_info else None
        )
        registration_date = None
        if case_info and "Registered on" in case_info:
            # Prefer the date immediately following "Registered on"
            m = re.search(r"Registered on\s*(\d{2}-\d{2}-\d{4})", case_info or "")
            if m:
                registration_date = m.group(1)

        verified_on = None
        if case_info and "Verified On" in case_info:
            verified_parts = case_info.split("Verified On", 1)
            verified_on = verified_parts[1].split("[", 1)[0].strip(" :")

        listed_info = _extract_td_text(soup, "Present/Last Listed On")
        # print('listed_info:', listed_info)
        listed_on = listed_info.split("[", 1)[0].strip() if listed_info else None
        status_info = _extract_td_text(soup, "Status/Stage")

        status = (
            status_info.split("List On", 1)[0].strip() if status_info else None
        )
        pending_or_disposed = 'pending' if 'pending' in status.lower() else 'disposed'
        category = _extract_td_text(soup, "Category")
        acts = [category]

        petitioners_raw = _extract_td_text(soup, "Petitioner(s)", separator="\n")
        petitioners = (
            [p.strip() for p in petitioners_raw.splitlines() if p.strip()]
            if petitioners_raw
            else None
        )
        respondents_raw = _extract_td_text(soup, "Respondent(s)", separator="\n")
        respondents = (
            [r.strip() for r in respondents_raw.splitlines() if r.strip()]
            if respondents_raw
            else None
        )
        petitioner_advocates_raw = _extract_td_text(
            soup, "Petitioner Advocate(s)", separator="\n"
        )
        petitioner_advocates = (
            [adv.strip() for adv in petitioner_advocates_raw.splitlines() if adv.strip()]
            if petitioner_advocates_raw
            else None
        )
        respondent_advocates_raw = _extract_td_text(
            soup, "Respondent Advocate(s)", separator="\n"
        )
        respondent_advocates = (
            [adv.strip() for adv in respondent_advocates_raw.splitlines() if adv.strip()]
            if respondent_advocates_raw
            else None
        )

        cin_no = _extract_td_text(soup, "CNR Number")

        judges = None
        if listed_info and "[" in listed_info and "]" in listed_info:
            judges_text = listed_info.split("[", 1)[1].split("]", 1)[0]
            judges = [j.strip() for j in judges_text.split("and") if j.strip()]

        listing_html = None
        listing_payload: Optional[dict[str, Any]] = None
        try:
            listing_html, listing_payload = _fetch_case_tab(
                diary_no, diary_year, "listing_dates"
            )
        except (requests.exceptions.RequestException, ValueError) as exc:
            logger.warning("Failed to fetch SCI listing dates: %s", exc)

        listings = _parse_listing_dates(listing_html) if listing_html else []

        judgement_html = None
        judgement_payload: Optional[dict[str, Any]] = None
        try:
            judgement_html, judgement_payload = _fetch_case_tab(
                diary_no, diary_year, "judgement_orders"
            )
        except (requests.exceptions.RequestException, ValueError) as exc:
            logger.warning("Failed to fetch SCI judgement/orders: %s", exc)

        orders = _parse_judgement_orders(judgement_html) if judgement_html else []

        return {
            "cin_no": cin_no,
            "registration_no": registration_no,
            "filling_no": diary_number,
            "case_no": case_number,
            "registration_date": registration_date,
            "filing_date": filing_date,
            "first_listing_date": verified_on,
            "next_listing_date": listed_on,
            "last_listing_date": None,
            "decision_date": None,
            "court_no": None,
            "disposal_nature": pending_or_disposed,
            "purpose_next": status,
            "case_type": category,
            "pet_name": ", ".join(petitioners) if petitioners else None,
            "res_name": ", ".join(respondents) if respondents else None,
            "petitioner_advocates": ", ".join(petitioner_advocates) if petitioner_advocates else None,
            "respondent_advocates": ", ".join(respondent_advocates) if respondent_advocates else None,
            "judges": ", ".join(judges) if judges else None,
            "bench_name": None,
            "court_name": None,
            "history": None,
            "acts": acts,
            "orders": orders,
            "listing_dates": listings,
            "additional_info": None,
            "original_json": {
                "case_details": case_payload,
                "listing_dates": listing_payload,
                "judgement_orders": judgement_payload,
            },
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


def parse_cause_list_table(soup):
    results = []
    table = soup.find("table")
    if not table:
        return []

    headers = [th.text.strip() for th in table.find_all("th")]
    rows = table.find_all("tr")[1:]

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue

        row_data = {}
        for i, cell in enumerate(cells):
            header = headers[i] if i < len(headers) else f"column_{i+1}"
            text = cell.text.strip()
            row_data[header] = text

            link = cell.find("a")
            if link and link.get("href"):
                row_data[f"{header}_url"] = link.get("href")

        results.append(row_data)
    return results


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def sci_get_cause_list(
    listing_date,
    list_type="daily",
    search_by="all_courts",
    causelist_type="Misc. Court",
    msb="both",
    **kwargs
):
    """
    Fetch cause list data from SCI.
    
    Args:
        listing_date (str): Date in DD-MM-YYYY format.
        list_type (str): 'daily' or 'other'. Defaults to 'daily'.
        search_by (str): 'all_courts', 'court', 'judge', 'aor_code', 'party_name'. Defaults to 'all_courts'.
        causelist_type (str): 'Misc. Court', 'Regular Court', etc. Defaults to 'Misc. Court'.
        msb (str): 'main', 'suppli', 'both'. Defaults to 'both'.
        **kwargs: Additional parameters like 'party_name', 'aor_code', 'court', 'judge'.
    """
    token_name, token_value, captcha_id, question, answer = init_captcha()
    data = {
        "action": "get_causes",
        "list_type": list_type,
        "search_by": search_by,
        "listing_date": listing_date,
        "causelist_type": causelist_type,
        "msb": msb,
        "scid": captcha_id,
        "siwp_captcha_value": answer,
        "tok_" + token_name: token_value,
        "es_ajax_request": "1",
        "submit": "Search",
    }
    data.update(kwargs)
    
    try:
        resp = _session_get(DATA_URL, params=data)
        response = validate_response(resp)
        
        if response.get("success"):
            # Check for resultsHtml first as per cause list response structure
            html_fragment = response.get("data", {}).get("resultsHtml")
            
            # Fallback to standard extraction if resultsHtml is missing/empty
            if not html_fragment:
                html_fragment = _extract_html_fragment(response)

            if not html_fragment:
                return []
            
            soup = BeautifulSoup(html_fragment, "html.parser")
            
            if "No records found" in soup.text:
                return []

            # Use a more generic table parser for cause lists
            if soup.find("table"):
                 return parse_cause_list_table(soup)
            
            return html_fragment # Return raw HTML if no table found
            
        else:
            return []

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise




def _pdf_normalize(text: str) -> str:
    return re.sub(r"[\s\-]+", "", text.upper())


def _is_sno_block(text: str, x0: float) -> bool:
    if x0 > 90:
        return False
    line = text.strip().splitlines()[0] if text.strip() else ""
    return bool(re.match(r"^\d{1,3}\b", line))


def _blocks_in_range(blocks, start_y: float, end_y: float):
    return [b for b in blocks if b[1] >= start_y and b[1] <= end_y]


def _blocks_to_lines(blocks) -> List[str]:
    lines: List[str] = []
    current_y = None
    current_line = []

    for b in blocks:
        y = b[1]
        if current_y is None or abs(y - current_y) <= 2:
            current_line.append(b)
            if current_y is None:
                current_y = y
        else:
            line_text = " ".join(
                [
                    t[4].strip().replace("\n", " ")
                    for t in sorted(current_line, key=lambda x: x[0])
                    if t[4].strip()
                ]
            ).strip()
            if line_text:
                lines.append(line_text)
            current_line = [b]
            current_y = y

    if current_line:
        line_text = " ".join(
            [
                t[4].strip().replace("\n", " ")
                for t in sorted(current_line, key=lambda x: x[0])
                if t[4].strip()
            ]
        ).strip()
        if line_text:
            lines.append(line_text)

    return lines


def _extract_lines_from_pdf_page(page: fitz.Page) -> List[Dict[str, Any]]:
    data = page.get_text("dict")
    lines: List[Dict[str, Any]] = []

    for block in data.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            spans = line.get("spans", [])
            text = " ".join(
                s["text"].strip() for s in spans if s.get("text", "").strip()
            )
            text = re.sub(r"\s+", " ", text).strip()
            if not text:
                continue
            x0, y0, x1, y1 = line.get("bbox", (0, 0, 0, 0))
            lines.append(
                {
                    "text": text,
                    "bbox": [x0, y0, x1, y1],
                    "x0": x0,
                    "y0": y0,
                    "x1": x1,
                    "y1": y1,
                }
            )

    lines.sort(key=lambda l: (l["y0"], l["x0"]))
    return lines


def sci_find_case_entries_in_pdf(pdf_path: str, case_number: str) -> List[Dict[str, Any]]:
    doc = fitz.open(pdf_path)
    target = _pdf_normalize(case_number)
    entries: List[Dict[str, Any]] = []

    for page_index, page in enumerate(doc):
        lines = _extract_lines_from_pdf_page(page)
        if not lines:
            continue

        match_indices = [
            i for i, line in enumerate(lines) if target in _pdf_normalize(line["text"])
        ]
        if not match_indices:
            continue

        sno_candidates = [
            i
            for i, line in enumerate(lines)
            if re.match(r"^\d{1,3}\b", line["text"])
        ]
        if sno_candidates:
            sno_min_x0 = min(lines[i]["x0"] for i in sno_candidates)
            sno_threshold = sno_min_x0 + 15
            sno_indices = [
                i for i in sno_candidates if lines[i]["x0"] <= sno_threshold
            ]
        else:
            sno_indices = []

        if sno_indices:
            seen_starts = set()
            for match_i in match_indices:
                start_index = max(
                    [i for i in sno_indices if i <= match_i], default=match_i
                )
                if start_index in seen_starts:
                    continue
                seen_starts.add(start_index)

                end_index = min(
                    [i for i in sno_indices if i > start_index],
                    default=len(lines),
                )
                row_lines = lines[start_index:end_index]

                if row_lines:
                    x0 = min(l["x0"] for l in row_lines)
                    y0 = min(l["y0"] for l in row_lines)
                    x1 = max(l["x1"] for l in row_lines)
                    y1 = max(l["y1"] for l in row_lines)
                else:
                    match_line = lines[match_i]
                    x0, y0, x1, y1 = match_line["bbox"]

                entries.append(
                    {
                        "page": page_index + 1,
                        "text": "\n".join(l["text"] for l in row_lines).strip(),
                        "bbox": [x0, y0, x1, y1],
                    }
                )
            continue

        blocks = page.get_text("blocks")
        if not blocks:
            continue

        matching_blocks = [b for b in blocks if target in _pdf_normalize(b[4])]
        if not matching_blocks:
            continue

        blocks_sorted = sorted(blocks, key=lambda b: (b[1], b[0]))

        for match in matching_blocks:
            match_y0 = match[1]
            end_y = page.rect.height

            for b in blocks_sorted:
                if b[1] <= match_y0 + 2:
                    continue
                if _is_sno_block(b[4], b[0]):
                    end_y = b[1] - 1
                    break

            start_y = max(0, match_y0 - 2)
            in_range = _blocks_in_range(blocks_sorted, start_y, end_y)
            lines = _blocks_to_lines(in_range)

            if in_range:
                x0 = min(b[0] for b in in_range)
                y0 = min(b[1] for b in in_range)
                x1 = max(b[2] for b in in_range)
                y1 = max(b[3] for b in in_range)
            else:
                x0, y0, x1, y1 = match[0], match[1], match[2], match[3]

            entries.append(
                {
                    "page": page_index + 1,
                    "text": "\n".join(lines).strip(),
                    "bbox": [x0, y0, x1, y1],
                }
            )

    return entries


def _fetch_order_document(url: str, referer: Optional[str] = None) -> requests.Response:
    headers = {}
    if referer:
        headers["Referer"] = referer
    return _session_get(url, headers=headers)


async def persist_orders_to_storage(
    orders: Optional[List[dict]],
    case_id: Optional[str] = None,
) -> Optional[List[dict]]:
    """
    Upload scraped SCI order documents to storage and update their URLs.
    """
    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        referer="https://www.sci.gov.in/",
    )


if __name__ == "__main__":
    # Test and Verify PDF Download
    logging.basicConfig(level=logging.INFO)
    
    # 1. Search for a case that is likely to have orders
    # using Case Type 4 (Civil Appeal), Number 963, Year 2017
    print("Searching for case...")
    cases = sci_search_by_case_number('4', '963', '2017')
    
    if not cases:
        print("No cases found.")
        exit(1)
        
    case = cases[0]
    print(f"Found case: {case}")
    
    diary_raw = case.get('cino') # e.g. "12345 / 2017"
    if not diary_raw:
        print("No Diary Number found in case.")
        exit(1)
        
    print(f"Diary Raw: {diary_raw}")
    try:
        diary_no, diary_year = diary_raw.split('/')
        diary_no = diary_no.strip()
        diary_year = diary_year.strip()
    except ValueError:
        print(f"Could not parse diary number: {diary_raw}")
        exit(1)
        
    # 2. Get Case Details to find Orders
    print(f"Fetching details for Diary No: {diary_no}, Year: {diary_year}")
    details = sci_get_details(diary_no, diary_year)
    
    if not details:
        print("Failed to fetch case details.")
        exit(1)
        
    orders = details.get('orders', [])
    print(f"Found {len(orders)} orders.")
    
    if not orders:
        print("No orders found for this case. Cannot verify PDF download.")
        exit(0)
        
    # 3. Try to download the first order
    first_order = orders[0]
    doc_url = first_order.get('document_url')
    print(f"Attempting to download order from: {doc_url}")
    
    try:
        # Use the helper function we added
        resp = _fetch_order_document(doc_url, referer="https://www.sci.gov.in/")
        
        if resp.status_code == 200:
            content_type = resp.headers.get('Content-Type', '')
            print(f"Download Status: {resp.status_code}")
            print(f"Content-Type: {content_type}")
            print(f"Content Length: {len(resp.content)} bytes")
            
            # Check for PDF signature
            if resp.content.startswith(b'%PDF-'):
                print("SUCCESS: Content verified as PDF.")
            else:
                print("WARNING: Content does not start with %PDF- signature.")
                print(f"First 100 bytes: {resp.content[:100]}")
        else:
            print(f"Failed to download. Status Code: {resp.status_code}")
            
    except Exception as e:
        print(f"Error during download: {e}")