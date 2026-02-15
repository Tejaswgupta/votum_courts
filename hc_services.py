

import json
import logging
import os
import re
from calendar import c
from datetime import datetime
from pathlib import Path
from urllib import parse

import ddddocr
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase.lib.client_options import ClientOptions
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

from supabase import Client, create_client

from .order_storage import \
    persist_orders_to_storage as _persist_orders_to_storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()

ORDER_BASE_URL='https://hcservices.ecourts.gov.in/hcservices/'

ocr = None
try:
    try:
        ocr = ddddocr.DdddOcr(show_ad=False, beta=True)
    except TypeError:
        try:
            ocr = ddddocr.DdddOcr(show_ad=False)
        except TypeError:
            ocr = ddddocr.DdddOcr()
except Exception:
    ocr = None
def _build_session() -> requests.Session:
    new_session = requests.session()
    new_session.verify = False
    new_session.headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Priority': 'u=0, i'
    }
    return new_session


session = _build_session()

CAPTCHA_URL = 'https://hcservices.ecourts.gov.in/hcservices/securimage/securimage_show.php?203'
DATA_URL = 'https://hcservices.ecourts.gov.in/hcservices/cases_qry/index_qry.php?action_code=showRecords'
DETAIL_URL = 'https://hcservices.ecourts.gov.in/hcservices/cases_qry/index_qry.php?action_code=get_case_details'
MAIN_PAGE_URL = 'https://hcservices.ecourts.gov.in/hcservices/main.php'

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")



def get_supabase_client() -> Client:
    """
    Lazy-init Supabase client for storing order PDFs.
    """

    try:
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as exc:  # pragma: no cover - defensive logging only
        print('exc', exc)
        logger.warning(f"Failed to initialize Supabase client: {exc}")
        _supabase_client = None

    return _supabase_client


def _refresh_session() -> None:
    """
    Refresh the global session and establish cookies by visiting the main page.
    """
    global session
    session = _build_session()
    try:
        session.get(MAIN_PAGE_URL, timeout=30)
    except Exception as exc:
        logger.warning("Failed to refresh HC session: %s", exc)


def _html_indicates_no_order(html: str) -> bool:
    if not html:
        return False
    lowered = html.lower()
    return "orders is not uploaded" in lowered or "order is not uploaded" in lowered


def _is_session_expired_html(html: str) -> bool:
    if not html:
        return False
    lowered = html.lower()
    return (
        "session expired" in lowered
        or "invalid session" in lowered
        or "securimage_show.php" in lowered
        or "please enter captcha" in lowered
    )


def _is_session_expired_response(resp: requests.Response) -> bool:
    if resp.url and "main.php" in resp.url:
        return True
    content_type = (resp.headers.get("content-type") or "").lower()
    if "text/html" not in content_type:
        return False
    if _html_indicates_no_order(resp.text):
        return False
    return _is_session_expired_html(resp.text)


def _request_with_session_refresh(method: str, url: str, **kwargs) -> requests.Response:
    response = session.request(method, url, **kwargs)
    if _is_session_expired_response(response):
        logger.warning("HC session expired. Refreshing session and retrying request.")
        _refresh_session()
        response = session.request(method, url, **kwargs)
    return response




def _fetch_order_document(order_url: str, referer: str | None) -> requests.Response:
    headers = session.headers.copy()
    if referer:
        headers["Referer"] = referer

    resp = _request_with_session_refresh(
        "GET",
        order_url,
        timeout=30,
        headers=headers,
    )

    content_type = resp.headers.get("content-type", "").lower()
    if "text/html" in content_type and _is_session_expired_html(resp.text):
        logger.warning(
            "Session expired while downloading order. Refreshing session and retrying."
        )
        _refresh_session()
        resp = _request_with_session_refresh(
            "GET",
            order_url,
            timeout=30,
            headers=headers,
        )

    return resp


async def persist_orders_to_storage(
    orders: list[dict] | None,
    case_id: str | None = None,
) -> list[dict] | None:
    return await _persist_orders_to_storage(
        orders,
        case_id=case_id,
        fetch_fn=_fetch_order_document,
        referer="https://hcservices.ecourts.gov.in/hcservices/cases_qry/index_qry.php",
    )


def parse_iso_date(date_str: str | None) -> str | None:
    """
    Helper to convert various date string formats to ISO 8601 (YYYY-MM-DD).
    Handles:
      - DD-MM-YYYY
      - DD/MM/YYYY
      - Dth Month YYYY (e.g. 10th January 2013)
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    s = date_str.strip()
    # Check for empty or placeholder
    if not s or s in ('--', '---', 'NA', 'N/A'):
        return None
        
    # Attempt 1: DD-MM-YYYY or DD/MM/YYYY
    # We can normalize separators first
    s_norm = s.replace('/', '-')
    try:
        dt = datetime.strptime(s_norm, '%d-%m-%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass
        
    # Attempt 2: "10th January 2013"
    # Regex to remove ordinal suffix from the day
    # We look for a digit(s) followed by st|nd|rd|th followed by space
    s_clean = re.sub(r'(\d+)(st|nd|rd|th)\s+', r'\1 ', s)
    
    try:
        dt = datetime.strptime(s_clean, '%d %B %Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass

    try:
        dt = datetime.strptime(s_clean, '%d %b %Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass
        
    # If all fails, return None (or original string if preferred)
    return None


def table_to_list(soup):
    """
    Parse the result table from HC Services response
    """
    table = []
    for row in soup.find_all('tr'):
        row_data = []
        for cell in row.find_all(['td', 'th']):
            row_data.append(cell.get_text().strip())
        if row_data:
            table.append(row_data)
    
    if len(table) < 2:
        return []
        
    data = []
    for row in table[1:]:
        data.append(dict(zip(table[0], row)))
    
    # Convert the data to a standard format
    return [{
        'cin_no': item.get('CIN No.', ''),
        'case_no': item.get('Case No.', ''),
        'registration_date': parse_iso_date(item.get('Registration Date', '')),
        'pet_name': item.get('Petitioner', ''),
        'res_name': item.get('Respondent', ''),
        'status': item.get('Status', ''),
        'last_hearing_date': parse_iso_date(item.get('Last Hearing Date', '')),
        'next_listing_date': parse_iso_date(item.get('Next Hearing Date', '')),
        'bench': item.get('Bench', ''),
    } for item in data]


def parse_json_response(json_response):
    """
    Parse the JSON response from HC Services
    """
    # Check if json_response is a dictionary
    if not isinstance(json_response, dict):
        return []
        
    if json_response.get('Error'):
        return []
    
    con_data = json_response.get('con', [])
    if not con_data:
        return []
    
    # The 'con' field might be a string-encoded JSON array that needs to be URL-decoded and parsed
    parsed_con_data = []
    for item in con_data:
        if isinstance(item, str):
            # URL decode if it's a string (like the example in logs)
            import urllib.parse
            decoded_item = urllib.parse.unquote(item)
            # Parse as JSON
            try:
                import json
                parsed_item = json.loads(decoded_item)
                if isinstance(parsed_item, list):
                    parsed_con_data.extend(parsed_item)
                elif isinstance(parsed_item, dict):
                    parsed_con_data.append(parsed_item)
            except json.JSONDecodeError:
                # If parsing fails, continue to next item
                continue
        elif isinstance(item, list):
            parsed_con_data.extend(item)
        elif isinstance(item, dict):
            parsed_con_data.append(item)
    
    # Convert the data to a standard format
    result = []
    for item in parsed_con_data:
        if isinstance(item, dict):
            result.append({
                'cin_no': item.get('cino', ''),
                'case_no': item.get('case_no', ''),
                'case_no2': item.get('case_no2', ''),
                'case_year': item.get('case_year', ''),
                'pet_name': item.get('pet_name', ''),
                'res_name': item.get('res_name', ''),
                'lpet_name': item.get('lpet_name', ''),
                'lres_name': item.get('lres_name', ''),
                'orderurlpath': item.get('orderurlpath', ''),
                'type_name': item.get('type_name', ''),
                'court_name': json_response.get('courtNameArr', [None])[0] if json_response.get('courtNameArr') else None,
                'court_code': json_response.get('court_code', [None])[0] if json_response.get('court_code') else None,
                'party_name1': item.get('party_name1', ''),
                'party_name2': item.get('party_name2', ''),
            })
        else:
            # Handle case where item is not a dict
            continue
    
    return result


def solve_captcha():
    """
    Get and solve the CAPTCHA from HC Services
    """
    try:
        captcha_image = _request_with_session_refresh("GET", CAPTCHA_URL).content
        captcha_result = ocr.classification(captcha_image)
        return captcha_result
    except Exception as e:
        logger.error(f"Error solving CAPTCHA: {e}")
        raise


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def hc_search_by_case_number(state_code, court_code, case_type, case_no, year):
    """
    Search for a case by case number in High Court Services
    """
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            captcha_result = solve_captcha()
            
            # Prepare data for the POST request
            data = {
                'court_code': court_code,
                'state_code': state_code,
                'court_complex_code': court_code,  # Usually same as court_code
                'caseStatusSearchType': 'CScaseNumber',
                'captcha': captcha_result,
                'case_type': case_type,
                'case_no': case_no,
                'rgyear': year,
                'caseNoType': 'new',
                'displayOldCaseNo': 'NO'
            }
            
            response = _request_with_session_refresh("POST", DATA_URL, data=data)
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} received, retrying...")
                retries += 1
                continue
                
            # Check if CAPTCHA was incorrect
            if "CAPTCHA" in response.text or "Invalid CAPTCHA" in response.text:
                logger.warning("Invalid CAPTCHA, retrying...")
                retries += 1
                continue
            
            # Parse the response
            try:
                # Try to parse as JSON first
                json_response = response.json()
                print('json_response', json_response)
                result = parse_json_response(json_response)
            except json.JSONDecodeError:
                # If JSON parsing fails, try parsing as HTML table
                soup = BeautifulSoup(response.text, 'html.parser')
                result = table_to_list(soup)
            
            print(result)
            if not result and retries < max_retries:
                logger.warning("No results found, retrying...")
                retries += 1
                continue
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            retries += 1
            if retries >= max_retries:
                raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            retries += 1
            if retries >= max_retries:
                raise
    
    raise Exception("Max retries reached. Could not fetch case data.")


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def hc_search_by_party_name(state_code, court_code, pet_name=None, res_name=None):
    """
    Search for cases by party name in High Court Services
    """
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            captcha_result = solve_captcha()
            
            # Prepare data for the POST request
            data = {
                'court_code': court_code,
                'state_code': state_code,
                'court_complex_code': court_code,
                'caseStatusSearchType': 'CSpartyName',
                'captcha': captcha_result,
                'pet_name': pet_name or '',
                'res_name': res_name or '',
                'displayOldCaseNo': 'NO'
            }
            
            response = _request_with_session_refresh("POST", DATA_URL, data=data)
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} received, retrying...")
                retries += 1
                continue
                
            # Check if CAPTCHA was incorrect
            if "CAPTCHA" in response.text or "Invalid CAPTCHA" in response.text:
                logger.warning("Invalid CAPTCHA, retrying...")
                retries += 1
                continue
                
            # Parse the response
            try:
                # Try to parse as JSON first
                json_response = response.json()
                result = parse_json_response(json_response)
            except json.JSONDecodeError:
                # If JSON parsing fails, try parsing as HTML table
                soup = BeautifulSoup(response.text, 'html.parser')
                result = table_to_list(soup)
            
            if not result and retries < max_retries:
                logger.warning("No results found, retrying...")
                retries += 1
                continue
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            retries += 1
            if retries >= max_retries:
                raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            retries += 1
            if retries >= max_retries:
                raise
    
    raise Exception("Max retries reached. Could not fetch case data.")


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def hc_get_case_details(state_code, court_code, case_id):
    """
    Get detailed information for a specific case
    """
    try:
        # For HC services, we might need to use a different endpoint or approach
        # based on how the system works. This is a placeholder implementation
        # that follows the general pattern.
        
        data = {
            'action_code': 'get_case_details',
            'court_code': court_code,
            'state_code': state_code,
            'case_id': case_id
        }
        
        response = _request_with_session_refresh("POST", DETAIL_URL, data=data)
        response.raise_for_status()
        
        # Try to parse as JSON first
        try:
            json_response = response.json()
            # If the response is JSON and has case details, return it
            if isinstance(json_response, dict):
                return {
                    "cin_no": json_response.get('cino'),
                    "case_no": json_response.get('case_no'),
                    "pet_name": json_response.get('pet_name'),
                    "res_name": json_response.get('res_name'),
                    "registration_date": parse_iso_date(json_response.get('registration_date')),
                    "status": json_response.get('status'),
                    "last_hearing_date": parse_iso_date(json_response.get('last_hearing_date')),
                    "next_listing_date": parse_iso_date(json_response.get('next_listing_date')),
                    "bench": json_response.get('bench'),
                    "court_name": json_response.get('court_name'),
                    "orderurlpath": json_response.get('orderurlpath'),
                    "original_json": json_response
                }
        except json.JSONDecodeError:
            # If JSON parsing fails, fall back to HTML parsing
            soup = BeautifulSoup(response.text, 'html.parser')
        
            # Extract detailed case information
            details = {
                "cin_no": None,
                "registration_no": None,
                "filling_no": None,
                "case_no": None,
                "registration_date": None,
                "filing_date": None,
                "first_listing_date": None,
                "next_listing_date": None,
                "last_listing_date": None,
                "decision_date": None,
                "court_no": None,
                "disposal_nature": None,
                "purpose_next": None,
                "case_type": None,
                "pet_name": None,
                "res_name": None,
                "advocates": None,
                "judges": None,
                "bench_name": None,
                "court_name": None,
                "history": [],
                "acts": None,
                "orders": [],
                "additional_info": response.text[:1000],  # Include partial response as additional info
                "original_json": None
            }
            
            # Try to parse case details from the HTML response
            # This is a general approach - actual parsing depends on the HTML structure
            tables = soup.find_all('table')
            if tables:
                # Parse the main case details table
                main_table = tables[0]
                rows = main_table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        key = cells[0].get_text().strip()
                        value = cells[1].get_text().strip()
                        # Map the key to the appropriate field in the details dict
                        
            return details
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error parsing case details: {e}")
        raise


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def hc_search_by_cnr(cnr_number):
    """
    Search for case details by CNR number in High Court Services
    """
    max_retries = 3
    retries = 0

    while retries < max_retries:
        try:
            captcha_result = solve_captcha()
            
            # Prepare data for the POST request based on the sample provided
            data = f"&captcha={captcha_result}&cino={cnr_number}&appFlag=web&action_code=fetchStateDistCourtNew&caseStatusSearchType=CNRNumber"
            
            # Set headers similar to the browser request
            headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en;q=0.9',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'priority': 'u=1, i',
                'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Brave";v="140"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'sec-gpc': '1',
                'x-requested-with': 'XMLHttpRequest',
                'Referer': 'https://hcservices.ecourts.gov.in/'
            }
            
            # Use the same endpoint as provided in the sample
            search_url = 'https://hcservices.ecourts.gov.in/hcservices/cases_qry/index_qry.php'
            
            response = _request_with_session_refresh(
                "POST",
                search_url,
                data=data,
                headers=headers,
            )
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} received, retrying...")
                retries += 1
                continue
                
            # Check if CAPTCHA was incorrect
            if "CAPTCHA" in response.text or "Invalid CAPTCHA" in response.text:
                logger.warning("Invalid CAPTCHA, retrying...")
                retries += 1
                continue
            
            case_history = parse_case_history(response.text)

            if not case_history.get('registration_no'):
                logger.warning(f"Registration number is blank. Retrying... ({retries + 1}/{max_retries})")
                retries += 1
                continue

            return case_history

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            retries += 1
            if retries >= max_retries:
                raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            retries += 1
            if retries >= max_retries:
                raise
    
    raise Exception("Max retries reached. Could not fetch case data.")


def parse_case_history(html_content):
    """
    Parse the HTML response from case history request to extract structured data
    in the same format as ecourts.py, while preserving additional information
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Initialize the result dictionary with the same structure as ecourts.py
    result = {
        'cin_no': None,
        'registration_no': None,
        'filling_no': None,
        'case_no': None,
        'registration_date': None,
        'filing_date': None,
        'first_listing_date': None,
        'next_listing_date': None,
        'last_listing_date': None,
        'decision_date': None,
        'court_no': None,
        'disposal_nature': None,
        'purpose_next': None,
        'case_type': None,
        'pet_name': None,
        'res_name': None,
        'advocates': None,
        'judges': None,
        'bench_name': None,
        'court_name': None,
        'history': [],
        'acts': [],
        'orders': [],
        'additional_info': None,
        'original_json': None  # Using this to store additional information
    }


    def _split_party_and_advocates(raw_text):
            """
            Given a raw string that may contain one or more party names followed by "Advocate" sections,
            return a tuple (party_names_list, advocates_list_or_None).

            Handles multiple numbered parties like:
                "1) NATIONAL COMPANY LAW TRIBUNAL Advocate-NOTICE... 2) GUJARAT OPERATIONAL... Advocate-MR. JAYDEEP..."
            Removes the markers like "1)" from the party names.
            Removes parenthesized numbers like "(3)" or "(6974)" from names and advocates.
            """
            if not raw_text:
                    return ([], None)

            def _clean_text(text):
                    if not text: return None
                    # Remove parenthesized numbers e.g. (3), (6974)
                    text = re.sub(r'\(\d+\)', '', text)
                    # Collapse whitespace
                    return " ".join(text.split())

            def _split_single(text):
                    # Normalize spaces first
                    text = " ".join(text.split())
                    
                    # Split on 'Advocate' (case-insensitive)
                    parts = re.split(r"(?i)\bAdvocate\s*[-:]?\s*", text, maxsplit=1)
                    
                    party_raw = parts[0]
                    party = _clean_text(party_raw)
                    
                    advs = []
                    if len(parts) > 1 and parts[1].strip():
                            adv_raw = parts[1].strip()
                            # split by comma or semicolon
                            candidates = re.split(r"[,;]", adv_raw)
                            for cand in candidates:
                                    cleaned = _clean_text(cand)
                                    if cleaned:
                                            advs.append(cleaned)
                    
                    return party, advs

            # Pattern to match numbering like "1)" but NOT "(1)"
            # Look for start of string, whitespace, OR closing parenthesis followed by digits and ")"
            # This handles cases where space might be missing like "...Advocate(123)2) Next Party"
            marker_pattern = r'(?:^|(?<=[\s)]))(\d+\))'
            
            if re.search(marker_pattern, raw_text):
                    # Split by numbered pattern, keeping the markers in the split result
                    blocks = re.split(marker_pattern, raw_text)
                    all_parties = []
                    all_advs = []

                    # blocks[0] is everything before the first "1)"
                    if blocks[0].strip():
                            p, a = _split_single(blocks[0])
                            if p: all_parties.append(p)
                            all_advs.extend(a)

                    # markers are at odd indices, content at even indices starting from 2
                    for i in range(1, len(blocks), 2):
                            content = blocks[i+1] if i+1 < len(blocks) else ""
                            p, a = _split_single(content)
                            if p:
                                    all_parties.append(p)
                            all_advs.extend(a)

                    # deduplicate advocates while preserving order
                    seen = set()
                    unique_advs = []
                    for adv in all_advs:
                            if adv not in seen:
                                    seen.add(adv)
                                    unique_advs.append(adv)
                    
                    return (all_parties, unique_advs if unique_advs else None)
            else:
                    p, a = _split_single(raw_text)
                    return ([p] if p else [], a if a else None)

    # Extract main case details
    case_details_table = soup.find('table', class_='case_details_table')
    if case_details_table:
        rows = case_details_table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 4:  # Each row has 4 cells (label, value, label, value)
                # First pair
                label1 = cells[0].get_text(strip=True)
                value1 = cells[1].get_text(strip=True)
                # Second pair
                label2 = cells[2].get_text(strip=True)
                value2 = cells[3].get_text(strip=True)
                
                # Map labels to standardized keys
                if 'Filing Number' in label1:
                    result['filling_no'] = value1
                if 'Filing Date' in label1:
                    result['filing_date'] = parse_iso_date(value1)
                if 'Registration Number' in label1 or 'Registration No.' in label1:
                    result['registration_no'] = value1
                if 'Registration Date' in label1:
                    result['registration_date'] = parse_iso_date(value1)
                
                if 'Filing Number' in label2:
                    result['filling_no'] = value2
                if 'Filing Date' in label2:
                    result['filing_date'] = parse_iso_date(value2)
                if 'Registration Number' in label2 or 'Registration No.' in label2:
                    result['registration_no'] = value2
                if 'Registration Date' in label2:
                    result['registration_date'] = parse_iso_date(value2)
    
    # Extract CNR number
    cnr_element = soup.find('strong', string=lambda x: x and 'CNR Number' in x)
    if cnr_element:
        cnr_td = cnr_element.find_parent('tr').find_all('td')[1] if cnr_element.find_parent('tr') else None
        if cnr_td:
            result['cin_no'] = cnr_td.get_text(strip=True)
    
    # Extract case status information
    status_table = soup.find('table', class_='table_r')
    if status_table:
        rows = status_table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                
                if 'First Hearing Date' in label:
                    result['first_listing_date'] = parse_iso_date(value)
                if 'Next Hearing Date' in label:
                    result['next_listing_date'] = parse_iso_date(value)
                if 'Decision Date' in label:
                    result['decision_date'] = parse_iso_date(value)
                if 'Case Status' in label:
                    result['status'] = value
                if 'Nature of Disposal' in label:
                    result['disposal_nature'] = value
                if 'Coram' in label:
                    result['judges'] = value
                if 'Bench Type' in label:
                    result['bench_name'] = value
                if 'State' in label:
                    result['state'] = value
                if 'District' in label:
                    result['court_name'] = value
    
    # Extract petitioner and respondent information
    petitioner_div = soup.find(string=lambda x: x and 'Petitioner and Advocate' in x)
    if petitioner_div:
        petitioner_parent = petitioner_div.find_parent('h2').find_next_sibling('span')
        if petitioner_parent:
            petitioner_all_name = petitioner_parent.get_text(separator=' ', strip=True)
            pet_name, pet_advs = _split_party_and_advocates(petitioner_all_name)
            result['pet_name'] = pet_name if pet_name else [petitioner_all_name.strip()]
            existing = result.get("advocates") or ""
            fragment = petitioner_all_name.strip()
            result["advocates"] = (
                "\n".join([x for x in [existing.strip(), f"Petitioner: {fragment}"] if x]).strip()
                or None
            )

    respondent_div = soup.find(string=lambda x: x and 'Respondent and Advocate' in x)
    if respondent_div:
        respondent_parent = respondent_div.find_parent('h2').find_next_sibling('span')
        if respondent_parent:
            respondent_all_name = respondent_parent.get_text(separator=' ', strip=True)
            res_name, res_advs = _split_party_and_advocates(respondent_all_name)
            result['res_name'] = res_name if res_name else [respondent_all_name.strip()]
            existing = result.get("advocates") or ""
            fragment = respondent_all_name.strip()
            result["advocates"] = (
                "\n".join([x for x in [existing.strip(), f"Respondent: {fragment}"] if x]).strip()
                or None
            )

    # Extract category details as case_type
    category_table = soup.find('table', id='subject_table')
    if category_table:
        rows = category_table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                
                if 'Category' in label:
                    result['case_type'] = value
    
    # Extract acts
    acts_table = soup.find('table', id='act_table')
    if acts_table:
        acts = []
        rows = acts_table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cells = row.find_all(['td'])
            if len(cells) >= 2:
                act = cells[0].get_text(strip=True)
                section = cells[1].get_text(strip=True)
                acts.append({'act': act, 'section': section})
        result['acts'] = acts
    
    # Extract hearing history - preserve additional information
    history_table = soup.find('table', class_='history_table')
    if history_table:
        hearing_history = []
        rows = history_table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cells = row.find_all(['td'])
            if len(cells) >= 5:
                hearing_entry = {
                    'judge': cells[1].get_text(strip=True),
                    'business_date': parse_iso_date(cells[2].get_text(strip=True)),
                    'hearing_date': parse_iso_date(cells[3].get_text(strip=True)),
                    'purpose': cells[4].get_text(strip=True),
                }
                
                # Extract cause list type which is in the first cell
                cause_list_type = cells[0].get_text(strip=True)
                if cause_list_type:
                    hearing_entry['cause_list_type'] = cause_list_type
                
                hearing_history.append(hearing_entry)
        result['history'] = hearing_history
    
    # Extract orders - preserve additional information
    order_table = soup.find('table', class_='order_table')
    if order_table:
        orders = []
        rows = order_table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cells = row.find_all(['td'])
            if len(cells) >= 5:
                # Prefer the explicit order_date cell if present, else fallback to order_on
                raw_order_date = cells[3].get_text(strip=True) or cells[1].get_text(strip=True)

                # Build a concise description from available fields: order details, judge and order number
                parts = []
                order_details = cells[4].get_text(strip=True)
                if order_details:
                    parts.append(order_details)
                judge = cells[2].get_text(strip=True)
                if judge:
                    parts.append(f"Judge: {judge}")
                order_number = cells[0].get_text(strip=True)
                if order_number:
                    parts.append(f"Order No: {order_number}")
                description = " | ".join(parts) if parts else None

                # Extract PDF/link if available and make absolute when possible
                pdf_link_tag = cells[4].find('a')
                document_url = pdf_link_tag.get('href') if pdf_link_tag else None

                if not document_url:
                    continue  # Skip if no document URL

                absolute_url = parse.urljoin(ORDER_BASE_URL, str(document_url)) if document_url else None

                # Append normalized order dict with required keys only
                orders.append({
                    'date': parse_iso_date(raw_order_date) or None,
                    'description': description or None,
                    'document_url': absolute_url or None
                })

        result['orders'] = orders

    # Extract IA (Interim Application) details
    ia_table = soup.find('table', class_='IAheading')
    if ia_table:
        ia_details = []
        rows = ia_table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cells = row.find_all(['td'])
            if len(cells) >= 5:
                ia_details.append({
                    'ia_number': cells[0].get_text(strip=True),
                    'party': cells[1].get_text(strip=True),
                    'filing_date': parse_iso_date(cells[2].get_text(strip=True)),
                    'next_date': parse_iso_date(cells[3].get_text(strip=True)),
                    'status': cells[4].get_text(strip=True)
                })
        if 'original_json' not in result or not result['original_json']:
            result['original_json'] = {}
        result['original_json']['ia_details'] = ia_details
    
    # Extract document details
    doc_table = soup.find('table', class_='transfer_table')
    if doc_table:
        documents = []
        rows = doc_table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cells = row.find_all(['td'])
            if len(cells) >= 6:
                documents.append({
                    'sr_no': cells[0].get_text(strip=True),
                    'doc_no': cells[1].get_text(strip=True),
                    'date_receiving': parse_iso_date(cells[2].get_text(strip=True)),
                    'filed_by': cells[3].get_text(strip=True),
                    'advocate_name': cells[4].get_text(strip=True),
                    'document_filed': cells[5].get_text(strip=True)
                })
        if 'original_json' not in result or not result['original_json']:
            result['original_json'] = {}
        result['original_json']['documents'] = documents
    
    # Extract objections
    obj_table = soup.find('table', class_='obj_table')
    if obj_table:
        objections = []
        rows = obj_table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cells = row.find_all(['td'])
            if len(cells) >= 5:
                objections.append({
                    'sr_no': cells[0].get_text(strip=True),
                    'scrutiny_date': parse_iso_date(cells[1].get_text(strip=True)),
                    'objection': cells[2].get_text(strip=True),
                    'compliance_date': parse_iso_date(cells[3].get_text(strip=True)),
                    'receipt_date': parse_iso_date(cells[4].get_text(strip=True))
                })
        if 'original_json' not in result or not result['original_json']:
            result['original_json'] = {}
        result['original_json']['objections'] = objections
    
    # Extract subordinate court information
    subordinate_div = soup.find(string=lambda x: x and 'Subordinate Court Information' in x)
    if subordinate_div:
        subordinate_parent = subordinate_div.find_parent('h2').find_next_sibling('span')
        if subordinate_parent:
            if 'original_json' not in result or not result['original_json']:
                result['original_json'] = {}
            result['original_json']['subordinate_court_info'] = subordinate_parent.get_text(strip=True)
    
    # Add HTML content as additional info
    result['additional_info'] = html_content[:1000]  # Include partial response as additional info
    
    return result


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def hc_get_case_history(state_code, court_code, court_complex_code, case_no, cino, app_flag=""):
    """
    Get case history for a specific case in High Court Services
    
    Args:
        state_code (str): State code
        court_code (str): Court code
        court_complex_code (str): Court complex code
        case_no (str): Case number
        cino (str): CIN number
        app_flag (str): Application flag (optional)
    
    Returns:
        dict: Parsed case history information
    """
    try:
        # Prepare data for the POST request
        data = {
            'court_code': court_code,
            'state_code': state_code,
            'court_complex_code': court_complex_code,
            'case_no': case_no,
            'cino': cino,
            'appFlag': app_flag
        }
        
        # Set headers similar to the browser request
        headers = {
            'accept': '*/*',
            'accept-language': 'en-GB,en;q=0.9',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Brave";v="140"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'x-requested-with': 'XMLHttpRequest',
            'Referer': 'https://hcservices.ecourts.gov.in/'
        }
        
        response = _request_with_session_refresh(
            "POST",
            'https://hcservices.ecourts.gov.in/hcservices/cases_qry/o_civil_case_history.php',
            data=data,
            headers=headers,
        )
        
        response.raise_for_status()
        
        # Parse the HTML response
        case_history = parse_case_history(response.text)
        
        return case_history
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error parsing case history: {e}")
        raise


def _parse_pipe_delimited_response(response_text: str, value_key: str, name_key: str) -> list[dict]:
    """
    Parse pipe-delimited format response (e.g., "0~Select#1~Value1#2~Value2").
    
    Args:
        response_text: The response text in pipe-delimited format
        value_key: Key name for the value in the returned dict (e.g., "court_code", "case_type_id")
        name_key: Key name for the name in the returned dict (e.g., "bench_name", "case_type_name")
    
    Returns:
        list[dict]: List of dictionaries with value_key and name_key
    """
    results = []
    if "~" in response_text and "#" in response_text:
        parts = response_text.split("#")
        for part in parts:
            if "~" in part:
                value, text = part.split("~", 1)
                value = value.strip()
                text = text.strip()
                # Skip placeholder options (value "0" or empty)
                if value and value != "0" and text:
                    results.append({value_key: value, name_key: text})
    return results


def _remove_duplicates_by_key(items: list[dict], key: str) -> list[dict]:
    """
    Remove duplicate dictionaries from a list based on a specific key.
    
    Args:
        items: List of dictionaries
        key: Key to use for duplicate detection
    
    Returns:
        list[dict]: List with duplicates removed
    """
    seen = set()
    unique_items = []
    for item in items:
        item_key = item.get(key)
        if item_key and item_key not in seen:
            seen.add(item_key)
            unique_items.append(item)
    return unique_items


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def hc_get_states():
    """
    Fetch all available High Court states from the HC services main page.
    
    Returns:
        list[dict]: [{ "state_code": str, "state_name": str }, ...]
    """
    states: list[dict] = []
    
    try:
        main_page = _request_with_session_refresh("GET", MAIN_PAGE_URL, timeout=30)
        if main_page.status_code == 200:
            soup = BeautifulSoup(main_page.text, "html.parser")
            
            # Look for state select dropdown - common names: state_code, state, hc_state
            state_select = soup.find(
                "select",
                attrs={"name": re.compile(r"state.*code|state|hc.*state", re.I)}
            )
            
            if state_select:
                logger.info(f"Found state select dropdown")
                for opt in state_select.find_all("option"):
                    value = (opt.get("value") or "").strip()
                    text = opt.get_text(strip=True)
                    # Skip placeholder options (value "0" or empty, or text like "Select State")
                    if value and value != "0" and text and text.lower() not in ["select", "select state", "select high court"]:
                        states.append({"state_code": value, "state_name": text})
                if states:
                    logger.info(f"Successfully extracted {len(states)} High Court states")
                    return states
            
            # Fallback: try to find any select with state-related options
            all_selects = soup.find_all("select")
            for select in all_selects:
                options = select.find_all("option")
                # Check if this looks like a state dropdown (has multiple options with numeric values)
                if len(options) > 5:  # State dropdowns typically have many options
                    for opt in options:
                        value = (opt.get("value") or "").strip()
                        text = opt.get_text(strip=True)
                        if value and value != "0" and value.isdigit() and text:
                            # Check if we already have this state_code
                            if not any(s["state_code"] == value for s in states):
                                states.append({"state_code": value, "state_name": text})
                    if states:
                        logger.info(f"Successfully extracted {len(states)} High Court states from fallback method")
                        return states
    except Exception as exc:
        logger.warning(f"Failed to fetch High Court states: {exc}", exc_info=True)
    
    return states


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def hc_get_benches(state_code: str):
    """
    Fetch bench (court_code) options for a given High Court state_code.

    Returns:
        list[dict]: [{ "court_code": str, "bench_name": str }, ...]
    """
    benches: list[dict] = []

    # First, visit the main page to establish session and get cookies
    try:
        main_page = _request_with_session_refresh("GET", MAIN_PAGE_URL, timeout=30)
        logger.info(f"Main page visit status: {main_page.status_code}")
    except Exception as exc:
        logger.warning(f"Failed to visit main page: {exc}")

    # Try the AJAX endpoint first (mirrors what the site calls on state change)
    try:
        # Mirror the exact browser call seen in Network tab for bench dropdown
        data = {
            "action_code": "fillHCBench",
            "state_code": state_code,
            "appFlag": "web",
        }
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-GB,en;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://hcservices.ecourts.gov.in",
            "Referer": "https://hcservices.ecourts.gov.in/hcservices/main.php",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest",
        }
        response = _request_with_session_refresh(
            "POST",
            "https://hcservices.ecourts.gov.in/hcservices/cases_qry/index_qry.php",
            data=data,
            headers=headers,
            timeout=30,
        )
        logger.info(f"Bench fetch response status: {response.status_code}, length: {len(response.text)}")
        logger.info(f"Bench fetch response text (first 1000 chars): {response.text[:1000]}")
        
        # Check for error messages in response
        if "THERE IS AN ERROR" in response.text.upper() or "ERROR" in response.text.upper()[:100]:
            logger.warning(f"Error detected in response: {response.text[:200]}")
        
        if response.status_code == 200:
            response_text = response.text.strip()
            
            # Try parsing pipe-delimited format first (e.g., "0~Select Bench#1~Court Name#2~Another Court")
            parsed_benches = _parse_pipe_delimited_response(response_text, "court_code", "bench_name")
            if parsed_benches:
                logger.info(f"Successfully extracted {len(parsed_benches)} benches from pipe-delimited format")
                return parsed_benches
            
            # Attempt JSON first
            try:
                payload = response.json()
                logger.info(f"Parsed JSON response keys: {list(payload.keys()) if isinstance(payload, dict) else 'not a dict'}")
                # Common shape: {"court": [{"court_code": "...", "court_name": "..."}], ...}
                if isinstance(payload, dict):
                    candidates = payload.get("court") or payload.get("courtArr") or payload.get("data") or []
                    logger.info(f"Found {len(candidates)} candidate benches")
                    for item in candidates:
                        if isinstance(item, dict):
                            code = str(item.get("court_code") or item.get("code") or item.get("value") or "").strip()
                            name = str(item.get("court_name") or item.get("name") or item.get("text") or "").strip()
                            if code and name:
                                benches.append({"court_code": code, "bench_name": name})
                    if benches:
                        logger.info(f"Successfully extracted {len(benches)} benches")
                        return benches
            except (json.JSONDecodeError, ValueError) as e:
                logger.debug(f"JSON decode failed: {e}, trying HTML parsing")

            # Fallback: parse HTML/options if returned
            soup = BeautifulSoup(response.text, "html.parser")
            options = soup.find_all("option")
            logger.info(f"Found {len(options)} option elements in HTML")
            for opt in options:
                value = (opt.get("value") or "").strip()
                text = opt.get_text(strip=True)
                if value and text and value != "0":
                    benches.append({"court_code": value, "bench_name": text})
            if benches:
                logger.info(f"Successfully extracted {len(benches)} benches from HTML")
                return benches
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(f"Bench fetch via AJAX failed: {exc}", exc_info=True)

    # Last resort: fetch main page and look for bench select (may already be populated)
    try:
        main_page = _request_with_session_refresh("GET", MAIN_PAGE_URL, timeout=30)
        if main_page.status_code == 200:
            soup = BeautifulSoup(main_page.text, "html.parser")
            bench_select = soup.find(
                "select",
                attrs={"name": re.compile("court_code|bench", re.I)}  # best-effort match
            )
            if bench_select:
                for opt in bench_select.find_all("option"):
                    value = (opt.get("value") or "").strip()
                    text = opt.get_text(strip=True)
                    if value and text and value != "0":
                        benches.append({"court_code": value, "bench_name": text})
                if benches:
                    return benches
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(f"Bench fetch via main page failed: {exc}")

    return benches


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def hc_get_case_types(state_code, court_code):
    """
    Get all available case types for a specific High Court bench by scraping.
    
    Args:
        state_code (str): State code for the High Court
        court_code (str): Court code (bench identifier)
    
    Returns:
        list: List of dictionaries with case type information:
            [
                {
                    "case_type_id": str,
                    "case_type_name": str
                },
                ...
            ]
    """
    max_retries = 5
    retries = 0
    
    while retries < max_retries:
        try:
            # Try to get case types from the HC website
            # The HC website typically provides case types via a dropdown or API endpoint
            # We'll try multiple approaches to get the case types
            
            # Approach 1: Try to get case types from the main page or a case type endpoint
            case_type_url = 'https://hcservices.ecourts.gov.in/hcservices/cases_qry/index_qry.php'
            
            # First, try to get the main page to see if case types are loaded there
            main_page_url = 'https://hcservices.ecourts.gov.in/hcservices/main.php'
            response = _request_with_session_refresh("GET", main_page_url, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} received, retrying...")
                retries += 1
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Try to find case type dropdown or select element
            case_types = []
            
            # Look for select elements with case type options
            select_elements = soup.find_all('select', {'id': re.compile(r'case.*type', re.I)})
            select_elements.extend(soup.find_all('select', {'name': re.compile(r'case.*type', re.I)}))
            
            for select in select_elements:
                options = select.find_all('option')
                for option in options:
                    value = option.get('value', '').strip()
                    text = option.get_text(strip=True)
                    if value and text and value != '' and value != '0' and value.lower() != 'select':
                        case_types.append({
                            "case_type_id": value,
                            "case_type_name": text
                        })
            
            # If we found case types, return them
            if case_types:
                # Remove duplicates based on case_type_id
                seen = set()
                unique_case_types = []
                for ct in case_types:
                    if ct["case_type_id"] not in seen:
                        seen.add(ct["case_type_id"])
                        unique_case_types.append(ct)
                return unique_case_types
            
            # Approach 2: Try to get case types via an API endpoint (similar to benches)
            try:
                # First visit main page to establish session
                _request_with_session_refresh("GET", MAIN_PAGE_URL, timeout=30)
                
                # Try to fetch case types using a POST request (similar pattern to benches)
                data = {
                    'action_code': 'fillCaseType',  # Common action code for case type dropdown
                    'state_code': state_code,
                    'court_code': court_code,
                    'appFlag': 'web',
                }
                headers = {
                    "Accept": "*/*",
                    "Accept-Language": "en-GB,en;q=0.9",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Origin": "https://hcservices.ecourts.gov.in",
                    "Referer": "https://hcservices.ecourts.gov.in/hcservices/main.php",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "X-Requested-With": "XMLHttpRequest",
                }
                
                response = _request_with_session_refresh(
                    "POST",
                    case_type_url,
                    data=data,
                    headers=headers,
                    timeout=30,
                )
                logger.info(f"Case type fetch response status: {response.status_code}, length: {len(response.text)}")
                logger.info(f"Case type fetch response text (first 1000 chars): {response.text[:1000]}")
                
                if response.status_code == 200:
                    response_text = response.text.strip()
                    
                    # Try parsing pipe-delimited format first (same as benches)
                    parsed_case_types = _parse_pipe_delimited_response(response_text, "case_type_id", "case_type_name")
                    if parsed_case_types:
                        unique_case_types = _remove_duplicates_by_key(parsed_case_types, "case_type_id")
                        logger.info(f"Successfully extracted {len(unique_case_types)} case types from pipe-delimited format")
                        return unique_case_types
                    
                    # Try JSON parsing
                    try:
                        json_response = response.json()
                        logger.info(f"Parsed JSON response keys: {list(json_response.keys()) if isinstance(json_response, dict) else 'not a dict'}")
                        if isinstance(json_response, dict):
                            candidates = json_response.get("case_type") or json_response.get("caseType") or json_response.get("data") or []
                            if isinstance(candidates, list):
                                for item in candidates:
                                    if isinstance(item, dict):
                                        code = str(item.get("case_type_id") or item.get("id") or item.get("code") or item.get("value") or "").strip()
                                        name = str(item.get("case_type_name") or item.get("name") or item.get("text") or "").strip()
                                        if code and name:
                                            case_types.append({
                                                "case_type_id": code,
                                                "case_type_name": name
                                            })
                                if case_types:
                                    logger.info(f"Successfully extracted {len(case_types)} case types from JSON")
                                    return case_types
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.debug(f"JSON decode failed: {e}, trying HTML parsing")
                    
                    # Fallback: parse HTML/options if returned
                    soup = BeautifulSoup(response.text, 'html.parser')
                    options = soup.find_all('option')
                    logger.info(f"Found {len(options)} option elements in HTML")
                    for option in options:
                        value = option.get('value', '').strip()
                        text = option.get_text(strip=True)
                        if value and text and value != '' and value != '0' and value.lower() != 'select':
                            case_types.append({
                                "case_type_id": value,
                                "case_type_name": text
                            })
                    if case_types:
                        unique_case_types = _remove_duplicates_by_key(case_types, "case_type_id")
                        logger.info(f"Successfully extracted {len(unique_case_types)} case types from HTML")
                        return unique_case_types
            except Exception as e:
                logger.warning(f"API approach for case types failed: {e}", exc_info=True)
            
            # Approach 3: Use the existing EcourtsService if available
            # This is a fallback that uses the encrypted API
            try:
                from .ecourts import EcourtsService
                ecourts_service = EcourtsService("HC", "3f91159bc5ba1090:in.gov.ecourts.eCourtsServices")
                case_types_result = ecourts_service.get_case_type(
                    court_code=court_code,
                    dist_code="",  # HC may not need dist_code
                    state_code=state_code
                )
                
                if case_types_result:
                    return [
                        {
                            "case_type_id": str(case_type_id),
                            "case_type_name": str(case_type_name)
                        }
                        for case_type_id, case_type_name in case_types_result
                    ]
            except Exception as e:
                logger.debug(f"EcourtsService approach failed: {e}")
            
            # If all approaches fail, return empty list
            if retries < max_retries - 1:
                logger.warning("No case types found, retrying...")
                retries += 1
                continue
            else:
                logger.warning(f"Could not fetch case types for state_code={state_code}, court_code={court_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            retries += 1
            if retries >= max_retries:
                raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            retries += 1
            if retries >= max_retries:
                raise
    
    return []


def test_hc_services():
    """
    Test function to validate HC Services scraper functionality
    """
    print("Testing HC Services scraper...")
    
    # Test CAPTCHA solving
    try:
        captcha = solve_captcha()
        print(f"CAPTCHA solved: {captcha}")
    except Exception as e:
        print(f"CAPTCHA solving failed: {e}")
        return False
    
    # Example test call (these values may need to be adjusted based on actual system)
    # Note: These are example calls - actual working parameters may differ
    try:
        # This is just a test to see if the function structure works
        # Actual values would need to be valid for the HC service
        result = hc_search_by_case_number(state_code=17, court_code=1, case_type=1, case_no=1, year=2025)
        print(f"Search by case number returned {len(result)} results")
        print(result[:2])  # Print first 2 results
    except Exception as e:
        print(f"Search by case number failed: {e}")
        return False
    
    return True


if __name__ == '__main__':
    # Run tests
    # success = test_hc_services()
    # if success:
    #     print("HC Services scraper tests completed successfully!")
    # else:
    #     print("HC Services scraper tests failed!")
    
    # Example usage:
    # cnr_number = "GJHC240000252024"
    # cnr_parts = cnr_number.split(',')
    # state_code, court_code, case_type, case_no, year = cnr_parts[:5]
    # Use hc_services to get case history based on CNR
    case_details = hc_search_by_cnr(
        "GJHC240325432025"  # Use the full CNR as cino
    )

    # print(json.dumps(case_details))
    # Convert response to the same format as before
    # from ecourts import convert_hc_response_to_json
    # formatted = convert_hc_response_to_json(case_details)
    # print(formatted)

    # sample = {
    # "case_id": "46",
    # "court_type": "High Court",
    # "orders": [
    #     {
    #         "date": "2025-10-28",
    #         "description": "View | Judge: NO COURT NO COURT AT PRESENT | Order No: 1",
    #         "document_url": "https://hcservices.ecourts.gov.in/hcservices/cases/display_pdf.php?filename=A9S7c5LDIsB6RXaCf816x19QTArYkO0Y9xX66ij8MzW7NKzYoAsDgOLfrqapDLA7&caseno=ARB.P./1783/2025&cCode=1&cino=DLHC010825562025&state_code=26&court_code=1&&appFlag="
    #     },
    #     {
    #         "date": "2025-11-19",
    #         "description": "View | Judge: HON'BLE MR. JUSTICE JASMEET SINGH | Order No: 2",
    #         "document_url": "https://hcservices.ecourts.gov.in/hcservices/cases/display_pdf.php?filename=A9S7c5LDIsB6RXaCf816x19QTArYkO0Y9xX66ij8MzWv6s8Hk5leYOGOSb%2B%2F8DgI&caseno=ARB.P./1783/2025&cCode=1&cino=DLHC010825562025&state_code=26&court_code=1&&appFlag="
    #     }
    # ]
    # }
    # res=persist_orders_to_storage(sample["orders"],sample['case_id'])  # Example function call to persist orders
    # print(res)
