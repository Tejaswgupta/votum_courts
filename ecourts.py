import base64
import html
import json
import random
import re
import time
from urllib import parse

import requests
from bs4 import BeautifulSoup
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from . import hc_services


def decode_response(result):
    key = bytes.fromhex("3273357638782F413F4428472B4B6250")
    print(result)
    iv_random = bytes.fromhex(result.strip()[:32])
    result_split = base64.b64decode(result.strip()[32:])

    cipher = AES.new(key, AES.MODE_CBC, iv_random)
    plaintext = unpad(cipher.decrypt(result_split), AES.block_size)
    s = plaintext.decode("utf-8")

    s = re.sub(r"\\n", r"\\n", s)
    s = re.sub(r"\\\'", r"\'", s)
    s = re.sub(r'\\"', r"\"", s)
    s = re.sub(r"\\&", r"\\&", s)
    s = re.sub(r"\\r", r"\\r", s)
    s = re.sub(r"\\t", r"\\t", s)
    s = re.sub(r"\\b", r"\\b", s)
    s = re.sub(r"\\f", r"\\f", s)
    s = re.sub(r"[\u0000-\u0019]+", "", s)

    return s


def generate_global_iv():
    a = [
        "556A586E32723575",
        "34743777217A2543",
        "413F4428472B4B62",
        "48404D635166546A",
        "614E645267556B58",
        "655368566D597133",
    ]
    global_index = random.randint(0, len(a) - 1)
    global_iv = a[global_index]
    return global_iv, global_index


def encrypt_data(data):
    data_encoded = json.dumps(data).encode()
    global_iv, global_index = generate_global_iv()
    random_iv = "".join(random.choices("0123456789abcdef", k=16))
    key = b"MbQeThWmZq4t6w9z"
    iv = bytes.fromhex(global_iv + random_iv)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encrypted_data = cipher.encrypt(pad(data_encoded, AES.block_size))
    encrypted_data = (
        random_iv + str(global_index) + base64.b64encode(encrypted_data).decode()
    )
    return encrypted_data


def request(url, params, jwt_token=None):
    headers = {}
    if jwt_token is not None:
        headers["Authorization"] = "Bearer " + encrypt_data(jwt_token)

    username = "sphlaqhi02"
    password = "d+6FG7rqX4qvhak6fC"
    proxy = f"https://{username}:{password}@in.smartproxy.com:10000"

    resp = requests.get(
        url=url,
        params={"params": encrypt_data(params)},
        headers=headers,
        # proxies={
        #     'http': proxy,
        #     'https': proxy
        # }
    )
    if resp.text.strip() == "null":
        print("response is null")
        # raise ValueError("Response is Null")
    if "error" in resp.text:
        print("Server returns error")
        # raise ValueError("Server returns error")
    return json.loads(decode_response(resp.text))


class EcourtsService:
    def __init__(self, type, uid):
        """
        `type` should be `DC` or `HC`.
        """
        self.__type: str = type
        self.__uid: str = uid

        url = f"https://app.ecourts.gov.in/ecourt_mobile_{self.__type}/appReleaseWebService.php"
        resp = request(url, {"uid": uid, "version": "3.0"})
        self.__jwt_token = resp["token"]

    def get_by_cnr(self, cnr_number):
        if self.__type == "HC":
            # For High Courts, use hc_services to search by CNR number directly
            # The CNR number format for High Courts can be like "GJHC240000252024"
            try:
                # Use the new hc_search_by_cnr function which uses the specific endpoint
                case_details = hc_services.hc_search_by_cnr(cnr_number=cnr_number)
                
                if case_details:
                    # If the search returns results, return the first detailed case info
                    # The function may return multiple cases, so we return the first one
                    # or process all results as needed
                    if isinstance(case_details, list) and len(case_details) > 0:
                        # Convert the first result to the format expected
                        return convert_hc_response_to_json(case_details[0])
                    elif isinstance(case_details, dict):
                        # If it's already a single dict result
                        return convert_hc_response_to_json(case_details)
                else:
                    # If no results, return an empty response
                    return {}
            except Exception as e:
                # Try the old method for backward compatibility
                cnr_parts = cnr_number.split(',')
                if len(cnr_parts) >= 5:
                    state_code, court_code, case_type, case_no, year = cnr_parts[:5]
                    # Use hc_services to get case history based on CNR
                    case_details = hc_services.hc_get_case_history(
                        state_code=state_code,
                        court_code=court_code,
                        court_complex_code=court_code,  # Often same as court_code
                        case_no=case_no,
                        cino=cnr_number  # Use the full CNR as cino
                    )
                    # Convert response to the same format as before
                    return convert_hc_response_to_json(case_details)
                else:
                    raise ValueError("Invalid CNR number format for High Court")
        elif self.__type == "DC":
            url = f"https://app.ecourts.gov.in/ecourt_mobile_{self.__type}/caseHistoryWebService.php"
            resp = request(
                url,
                {
                    "cinum": cnr_number,
                    "language_flag": "english",
                    "bilingual_flag": "0",
                },
                self.__jwt_token,
            )
            return convert_response_json(self.__type, resp)

    def get_districts_list(self, state_code):
        if self.__type == "HC":
            # For High Courts, we may need to get list of benches/courts instead of districts
            # Since hc_services doesn't provide this directly, we return an empty list or handle as needed
            # This function might not be applicable to High Courts in the same way as District Courts
            return []
        elif self.__type == "DC":
            url = f"https://app.ecourts.gov.in/ecourt_mobile_{self.__type}/districtWebService.php"
            resp = request(
                url,
                {
                    "action_code": "benches",
                    "state_code": state_code,
                    "test_param": "pending",
                    "uid": self.__uid,
                },
                self.__jwt_token,
            )
            return resp["districts"]

    def get_state_list(self):
        if self.__type == "HC":
            # For High Courts, since hc_services doesn't provide a state list function,
            # we return a list of known state codes for HC services
            # These would typically be pre-defined based on the HC system
            # This is a placeholder - in reality you'd need to determine this based on available HC systems
            return [{"state_code": "1", "state_name": "Andhra Pradesh"},
                    {"state_code": "2", "state_name": "Arunachal Pradesh"},
                    {"state_code": "3", "state_name": "Assam"},
                    {"state_code": "4", "state_name": "Bihar"},
                    {"state_code": "5", "state_name": "Chhattisgarh"},
                    {"state_code": "6", "state_name": "Goa"},
                    {"state_code": "7", "state_name": "Gujarat"},
                    {"state_code": "8", "state_name": "Haryana"},
                    {"state_code": "9", "state_name": "Himachal Pradesh"},
                    {"state_code": "10", "state_name": "Jammu and Kashmir"},
                    {"state_code": "11", "state_name": "Jharkhand"},
                    {"state_code": "12", "state_name": "Karnataka"},
                    {"state_code": "13", "state_name": "Kerala"},
                    {"state_code": "14", "state_name": "Madhya Pradesh"},
                    {"state_code": "15", "state_name": "Maharashtra"},
                    {"state_code": "16", "state_name": "Manipur"},
                    {"state_code": "17", "state_name": "Meghalaya"},
                    {"state_code": "18", "state_name": "Mizoram"},
                    {"state_code": "19", "state_name": "Nagaland"},
                    {"state_code": "20", "state_name": "Odisha"},
                    {"state_code": "21", "state_name": "Punjab"},
                    {"state_code": "22", "state_name": "Rajasthan"},
                    {"state_code": "23", "state_name": "Sikkim"},
                    {"state_code": "24", "state_name": "Tamil Nadu"},
                    {"state_code": "25", "state_name": "Telangana"},
                    {"state_code": "26", "state_name": "Tripura"},
                    {"state_code": "27", "state_name": "Uttar Pradesh"},
                    {"state_code": "28", "state_name": "Uttarakhand"},
                    {"state_code": "29", "state_name": "West Bengal"},
                    {"state_code": "30", "state_name": "Andaman and Nicobar Islands"},
                    {"state_code": "31", "state_name": "Chandigarh"},
                    {"state_code": "32", "state_name": "Dadra and Nagar Haveli"},
                    {"state_code": "33", "state_name": "Daman and Diu"},
                    {"state_code": "34", "state_name": "Lakshadweep"},
                    {"state_code": "35", "state_name": "Delhi"},
                    {"state_code": "36", "state_name": "Puducherry"}]
        elif self.__type == "DC":
            url = f"https://app.ecourts.gov.in/ecourt_mobile_{self.__type}/stateWebService.php"
            resp = request(
                url, {"action_code": "fillState", "time": time.time()}, self.__jwt_token
            )
            return resp["states"]

    def get_complex_list(self, state_code, dist_code):
        if self.__type != "DC":
            raise ValueError("The function is not implemented for High Courts")
        try:
            url = f"https://app.ecourts.gov.in/ecourt_mobile_DC/courtEstWebService.php"
            resp = request(
                url,
                {
                    "action_code": "fillCourtComplex",
                    "dist_code": dist_code,
                    "state_code": state_code,
                },
                self.__jwt_token,
            )
            result = []
            for complex in resp["courtComplex"]:
                for court in complex[complex["njdg_est_code"]].values():
                    court["complex_code"] = complex["complex_code"]
                    court["court_complex_name"] = complex["court_complex_name"]
                    court["dist_code"] = complex["njdg_dist_code"]
                    court["state_code"] = complex["njdg_state_code"]
                    result.append(court)
            return result

        except Exception:
            return None

    def search_by_advocate_name(
        self,
        dist_code,
        state_code,
        checked_search_by_radio_value,
        court_code="",
        pending_disposed="",
        advocate_name="",
        barcode="",
        barstatecode="",
        date="",
        year="",
    ):
        """
        `court_code` is only required by High Courts; `court_code_arr` is only required by District Courts.
        `checked_search_by_radio_value`'s value should be one of the following: `1`(Advocate Name), `2`(Bar Code) and `3`(Date Case List).
        `pending_disposed`'s value should be one of the following: `Pending`, `Disposed` and `Both`.
        `advocate_name` and `pending_disposed` are required by `Advocate Name` search.
        `barstatecode`, `barcode`, `year` and `pending_disposed` are required by `Bar Code` search.
        `barstatecode`, `barcode`, `year` and `date` are required by `Date Case List` search.
        """
        if self.__type == "HC":
            # For High Courts, use hc_services to search by party name (since advocates name search may not be directly supported)
            # This is an approximation - we'll search by party name if advocate name is provided
            if checked_search_by_radio_value == "1":  # Advocate Name search
                # Try searching by petitioner or respondent name using the advocate name
                result = hc_services.hc_search_by_party_name(
                    state_code=state_code,
                    court_code=court_code,
                    pet_name=advocate_name,
                    res_name=advocate_name
                )
                return result
            elif checked_search_by_radio_value == "3":  # Date Case List
                # Search by hearing date
                result = hc_services.hc_search_by_hearing_date(
                    state_code=state_code,
                    court_code=court_code,
                    hearing_date=date
                )
                return result
            else:
                # For other search types, return empty result or handle as needed
                return []
        elif self.__type == "DC":
            url = f"https://app.ecourts.gov.in/ecourt_mobile_{self.__type}/searchByAdvocateName.php"
            params = {
                "advocateName": advocate_name,
                "barcode": barcode,
                "barstatecode": barstatecode,
                "checkedSearchByRadioValue": checked_search_by_radio_value,
                "date": date,
                "dist_code": dist_code,
                "pendingDisposed": pending_disposed,
                "state_code": state_code,
                "year": year,
            }
            params["court_code_arr"] = court_code
            params["bilingual_flag"] = "0"
            params["language_flag"] = "english"
            resp = request(url, params, self.__jwt_token)
            return resp

    def get_case_type(self, court_code, dist_code, state_code):
        url = f"https://app.ecourts.gov.in/ecourt_mobile_{self.__type}/caseNumberWebService.php"
        params = {
            "court_code": court_code,
            "dist_code": dist_code,
            "state_code": state_code,
        }
        if self.__type == "DC":
            params["bilingual_flag"] = "0"
            params["language_flag"] = "english"
        resp = request(url, params, self.__jwt_token)
        # print(resp)
        data = {}
        for line in resp["case_types"]:
            if not line.get("case_type"):
                continue
            for item in line["case_type"].split("#"):
                type_id, type_name = item.split("~")
                data[type_id] = type_name
        return list(data.items())

    def search_by_case_number(
        self, case_number, case_type, dist_code, state_code, year, court_code
    ):
        """
        `court_code` is only required by High Courts; `court_code_arr` is only required by District Courts.
        """
        url = f"https://app.ecourts.gov.in/ecourt_mobile_{self.__type}/caseNumberSearch.php"
        params = {
            "case_number": case_number,
            "case_type": case_type,
            "court_code": court_code,
            "dist_code": dist_code,
            "state_code": state_code,
            "year": year,
        }
        if self.__type == "DC":
            params["court_code_arr"] = court_code
            params["bilingual_flag"] = "0"
            params["language_flag"] = "english"
        elif self.__type == "HC":
            params["court_code"] = court_code
        resp = request(url, params, self.__jwt_token)

        print(resp)

        # Safely access keys to avoid KeyError when API response shape varies
        if self.__type == "DC":
            zero = resp.get("0", {})
            case_nos = zero.get("caseNos")
            if case_nos:
                return case_nos
        elif self.__type == "HC":
            case_nos = resp.get("caseNos")
            if case_nos:
                return case_nos

        # Default empty list when no case numbers found or unexpected response
        return []


def convert_response_json(type, response_json):
    concise_json = {}
    history = response_json["history"]

    concise_json["cin_no"] = history["cino"]
    concise_json["registration_no"] = history["reg_no"]
    concise_json["filling_no"] = history["fil_no"]
    concise_json["case_no"] = history["case_no"]
    concise_json["registration_date"] = history["dt_regis"]
    concise_json["filing_date"] = history["date_of_filing"]
    concise_json["first_listing_date"] = history["date_first_list"]
    concise_json["next_listing_date"] = history["date_next_list"]
    concise_json["last_listing_date"] = history["date_last_list"]
    concise_json["decision_date"] = history["date_of_decision"]
    concise_json["court_no"] = history["court_no"]

    concise_json["disposal_nature"] = history["disp_nature"]
    concise_json["purpose_next"] = history["purpose_name"]
    concise_json["case_type"] = history["type_name"]
    concise_json["pet_name"] = history["pet_name"]
    concise_json["res_name"] = history["res_name"]
    concise_json["petitioner_advocates"] = history["pet_adv"]
    concise_json["respondent_advocates"] = history["res_adv"]

    if type == "HC":
        concise_json["judges"] = history["court_judge"]
        concise_json["bench_name"] = history["district_name"]
        concise_json["court_name"] = history["state_name"]
        concise_json["history"] = convert_hearing_to_json(
            "HC", history["historyOfCaseHearing"]
        )

    else:
        concise_json["judges"] = history["desgname"]
        concise_json["bench_name"] = history["desgname"]
        concise_json["court_name"] = history["court_name"]
        concise_json["history"] = convert_hearing_to_json(
            "DC", history["historyOfCaseHearing"]
        )

    # Convert HTML fields to JSON
    concise_json["acts"] = convert_act_json(history["act"])

    concise_json["orders"] = (
        convert_order_to_json(history.get("interimOrder"))
        + convert_order_to_json(history.get("finalOrder"))
    )

    concise_json["additional_info"] = None  # Not present in the response JSON
    concise_json["original_json"] = response_json

    return concise_json


def convert_act_json(act):
    if act is None:
        return

    soup = BeautifulSoup(act, "html.parser")
    table = soup.find("table")
    headers = [th.text.strip() for th in table.find("tr").find_all("th")]
    data = []
    for row in table.find_all("tr")[1:]:
        row_data = [td.text.strip() for td in row.find_all("td")]
        data.append(dict(zip(headers, row_data)))
    return data


def convert_hearing_to_json(type, hearing):
    if hearing is None:
        return

    soup = BeautifulSoup(hearing, "html.parser")
    table = soup.find("table")
    data = []

    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")

        if type == "HC":
            judge = cols[1].text.strip() if cols[1] else ""
            business_date = cols[2].text.strip() if cols[2] else ""
            hearing_date = cols[3].text.strip() if cols[3] else ""
            purpose = cols[4].text.strip() if cols[4] else ""
            data.append(
                {
                    "judge": judge,
                    "business_date": business_date,
                    "hearing_date": hearing_date,
                    "purpose": purpose,
                }
            )

        else:
            judge = cols[0].text.strip() if cols[0] else ""
            business_date = cols[1].text.strip() if cols[1] else ""
            hearing_date = cols[2].text.strip() if cols[2] else ""
            purpose = cols[3].text.strip() if cols[3] else ""
            data.append(
                {
                    "judge": judge,
                    "business_date": business_date,
                    "hearing_date": hearing_date,
                    "purpose": purpose,
                }
            )

    return data


def convert_order_to_json(order):
    if order is None:
        return []

    soup = BeautifulSoup(order, "html.parser")
    table = soup.find("table")
    data = []
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        # Safely extract columns
        order_number = cols[0].text.strip() if len(cols) > 0 else ""
        order_date = cols[1].text.strip() if len(cols) > 1 else ""

        # Description: prefer any textual details present in the link cell or fallback to order number
        description = None
        if len(cols) > 2:
            description = cols[2].text.strip()
        if not description:
            description = f"Order No: {order_number}" if order_number else None

        # Extract href if present and make absolute
        document_url = href = cols[2].find("a").get("href")

        data.append({"date": order_date or None, "description": description or None, "document_url": document_url or None})

    return data


if __name__ == "__main__":
    dc = EcourtsService("DC", "3f91159bc5ba1090:in.gov.ecourts.eCourtsServices")
    state = dc.get_state_list()[1]
    print("state", state)
    district = dc.get_districts_list(state["state_code"])[2]
    print("district", district)
    complex = dc.get_complex_list(state["state_code"], district["dist_code"])[0]
    print("complex", complex)
    case_type, case_type_name = dc.get_case_type(
        complex["court_code"], district["dist_code"], state["state_code"]
    )[0]
    print("case_type", case_type, case_type_name)

    search_result = dc.search_by_case_number(
        case_number="1",
        case_type=case_type,
        dist_code=district["dist_code"],
        state_code=state["state_code"],
        year="2023",
        court_code=complex["court_code"],
    )
    print("search_result", search_result)
    print("*" * 50)

    hc = EcourtsService("HC", "3f91159bc5ba1090:in.gov.ecourts.eCourtsServices")
    state = hc.get_state_list()
    print("state", state)
    district = hc.get_districts_list(state[0]["state_code"])
    print("district", district)
    # complex = hc.get_complex_list(state[0]['state_code'], district[0]['dist_code'])
    # print(complex)
    # case_type, case_type_name = hc.get_case_type(complex['court_code'], district['dist_code'], state['state_code'])[0]
    # print(case_type, case_type_name)
    search_result = hc.search_by_case_number(
        case_number="1",
        case_type="90",
        dist_code="1",
        state_code="13",
        year="2023",
        court_code="1",
    )
    print("search_result", search_result)


def convert_hc_response_to_json(hc_response):
    """
    Convert the HC response from hc_services to the same format as DC response
    """
    if not hc_response:
        return {}
    
    # Initialize result with default values
    result = {
        'cin_no': hc_response.get('cin_no'),
        'registration_no': hc_response.get('registration_no'),
        'filling_no': hc_response.get('filling_no'),
        'case_no': hc_response.get('case_no'),
        'registration_date': hc_response.get('registration_date'),
        'filing_date': hc_response.get('filing_date'),
        'first_listing_date': hc_response.get('first_listing_date'),
        'next_listing_date': hc_response.get('next_listing_date'),
        'last_listing_date': hc_response.get('last_listing_date'),
        'decision_date': hc_response.get('decision_date'),
        'court_no': hc_response.get('court_no'),
        'disposal_nature': hc_response.get('disposal_nature'),
        'status': hc_response.get('status'),
        'purpose_next': hc_response.get('purpose_next'),
        'case_type': hc_response.get('case_type'),
        'pet_name': hc_response.get('pet_name'),
        'res_name': hc_response.get('res_name'),
        'petitioner_advocates': hc_response.get('petitioner_advocates'),
        'respondent_advocates': hc_response.get('respondent_advocates'),
        'judges': hc_response.get('judges'),
        'bench_name': hc_response.get('bench_name'),
        'court_name': hc_response.get('court_name'),
        'history': hc_response.get('history', []),
        'acts': hc_response.get('acts', []),
        'orders': [],
        'additional_info': hc_response.get('additional_info'),
        'original_json': hc_response.get('original_json')
    }
    
    # Normalize HC orders: hc_response['orders'] may already be a list or a dict with interim/final keys
    raw_orders = hc_response.get('orders')

    def _normalize_order_item(item):
        # Ensure item is a dict and map common keys to the desired output
        if not isinstance(item, dict):
            return {'date': None, 'description': str(item), 'document_url': None}

        date = item.get('date') or item.get('order_date') or item.get('order_on') or item.get('orderOn') or None
        # Document url may be under different keys
        document_url = item.get('document_url') or item.get('link') or item.get('orderurlpath') or None
        if document_url and not document_url.startswith('http'):
            try:
                document_url = parse.urljoin('https://hcservices.ecourts.gov.in/', document_url)
            except Exception:
                pass

        description = item.get('description') or item.get('order_details') or item.get('order_number') or ''
        # Append judge if present for context
        judge = item.get('judge')
        if judge:
            description = (description + f" | Judge: {judge}").strip()

        return {'date': date or None, 'description': description or None, 'document_url': document_url or None}

    orders_list = []
    if isinstance(raw_orders, dict):
        interim = raw_orders.get('interim_order') or raw_orders.get('interimOrder') or []
        final = raw_orders.get('final_order') or raw_orders.get('finalOrder') or []
        # normalize
        for o in (interim + final):
            orders_list.append(_normalize_order_item(o))
    elif isinstance(raw_orders, list):
        for o in raw_orders:
            orders_list.append(_normalize_order_item(o))
    else:
        orders_list = []

    result['orders'] = orders_list

    return result
