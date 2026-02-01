import json
import logging
import re

import pandas as pd
import requests
from fake_useragent import UserAgent
from search import ConciseJson , HistoryData , Order

ua = UserAgent()

URL = "https://nclat.nic.in/display-board/cases_details"
VIEW_URL = "https://nclat.nic.in/display-board/view_details"
SEARCH_BY = "party_wise"
LOCATION = "delhi"
PARTY_NAME = "Singh"
PARTY_TYPE = "1"


logger = logging.getLogger(__name__)


def parse_api_response(api_response: dict) -> ConciseJson:
    data = api_response['data']
    case_details = data['case_details'][0]
    party_details = data['party_details']
    first_hearing_details = data['first_hearing_details']
    case_history = data['case_history']
    order_history = data['order_history']

    concise_json = ConciseJson(
        case_no=case_details['case_no'],
        registration_date=case_details['registration_date'],
        filing_date=case_details['date_of_filing'],
        court_no=str(first_hearing_details['court_no']),
        case_type=case_details['case_type'],
        next_listing_date=data['next_hearing_details']['1'],
        petitioner=', '.join([applicant['name'] for applicant in party_details['applicant_name']]),
        petitioner_advocates= ', '.join(data['legal_representative']['applicant_legal_representative_name']),
        respondent=', '.join([respondent['name'] for respondent in party_details['respondant_name']]),
        respondent_advocates = ', '.join(data['legal_representative']['respondent_legal_representative_name']),
        judges=first_hearing_details['coram'],
        history=[HistoryData(hearing_date=history['hearing_date'], purpose=history['purpose']) for history in case_history],
        order_history=[Order(hearing_date=order['order_date'], purpose=order['order_type'], order_link=order['order_pdf_download']) for order in order_history],
    )
    return concise_json


class Main:
    def __init__(self):
        self.url = ""
        self.token = ""
        self.xsrf = ""
        self.laravel_session = ""
        self.numbers = []
        self.headers = {}
        self.result = []
        self.data = []
        self.view_data = []

    def get_security_tokens(self):
        main = requests.get(URL)
        url_data = main.text
        try:
            self.token = re.findall(
                '<meta name="csrf-token" content="(\w+)" />', url_data
            )[0]
        except KeyError:
            logger.error("Token is missing in the page!")
        self.xsrf = main.cookies.get("XSRF-TOKEN")
        self.laravel_session = main.cookies.get("laravel_session")
        self.headers = {
            "User-Agent": ua.random,
            "Accept": "*/*",
            "Accept-Language": "en",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://nclat.nic.in",
            "Connection": "keep-alive",
            "Referer": "https://nclat.nic.in/display-board/cases",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": f"XSRF-TOKEN={self.xsrf}; laravel_session={self.laravel_session}",
        }

    def get_main_data(self):
        self.url = URL
        payload = f"_token={self.token}&search_by={SEARCH_BY}&location={LOCATION}&case_type=&exact_search_word=1&text_name=&from_date=&to_date=&case_status=&select_party={PARTY_TYPE}&party_name={PARTY_NAME}&diary_no=&case_number={PARTY_NAME}&advocate_name=&case_year=All"
        request_server = requests.post(
            self.url, data=payload, headers=self.headers)
        all_data = json.loads(request_server.text)

        print(all_data)

        for value in all_data["data"]:
            if value[5] != "Pending":
                value[5] = "Disposed"
            self.numbers.append(value[1])
            self.data.append(
                {
                    "cin_no": value[1],
                    "case_no": value[2].split('/')[0],
                    'case_year': value[2].split('/')[-1],
                    "case_title": value[3],
                    "registration_date": value[4],
                    "status": value[5],
                }
            )
            

    def get_view_data(self,number):
        self.url = VIEW_URL
        payload = f"search_type=view_details&filing_no={number}&_token={self.token}&bench_name=delhi"
        request_server = requests.post(
            VIEW_URL, headers=self.headers, data=payload)

        all_view_data = json.loads(request_server.text)
        value = all_view_data["data"]
        try:
            stage_case = value["case_history"][0]["stage_of_case"]
        except (KeyError, IndexError, TypeError):
            stage_case = ""
        try:
            first_hearing_details_hearing_date = value["first_hearing_details"][
                "hearing_date"
            ]
        except (KeyError, IndexError, TypeError):
            first_hearing_details_hearing_date = ""
        try:
            case_year = value["case_details"][0]["case_year"]
        except (KeyError, IndexError, TypeError):
            case_year = ""
        try:
            case_type = value["case_details"][0]["case_type"]
        except (KeyError, IndexError, TypeError):
            case_type = ""
        try:
            first_hearing_details_coram = value["first_hearing_details"]["coram"]
        except (KeyError, IndexError, TypeError):
            first_hearing_details_coram = ""

        try:
            first_hearing_details_stage_of_case = value["first_hearing_details"][
                "stage_of_case"
            ]
        except (KeyError, IndexError, TypeError):
            first_hearing_details_stage_of_case = ""
        try:
            last_hearing_details_hearing_date = value["last_hearing_details"][
                "hearing_date"
            ]
        except (KeyError, IndexError, TypeError):
            last_hearing_details_hearing_date = ""
        try:
            last_hearing_details_coram = value["last_hearing_details"]["coram"]
        except (KeyError, IndexError, TypeError):
            last_hearing_details_coram = ""
        try:
            last_hearing_details_stage_of_case = value["last_hearing_details"][
                "stage_of_case"
            ]
        except (KeyError, IndexError, TypeError):
            last_hearing_details_stage_of_case = ""
        try:
            next_hearing_details_hearing_date = value["next_hearing_details"][
                "hearing_date"
            ]
        except (KeyError, IndexError, TypeError):
            next_hearing_details_hearing_date = ""
        try:
            next_hearing_details_coram = value["next_hearing_details"]["coram"]
        except (KeyError, IndexError, TypeError):
            next_hearing_details_coram = ""
        try:
            next_hearing_details_stage_of_case = value["next_hearing_details"][
                "stage_of_case"
            ]
        except (KeyError, IndexError, TypeError):
            next_hearing_details_stage_of_case = ""
        try:
            case_history_hearing_date = value["case_history"][0]["hearing_date"]
        except (KeyError, IndexError, TypeError):
            case_history_hearing_date = ""
        try:
            date_of_filing = value["case_details"][0]["date_of_filing"]
        except (KeyError, IndexError, TypeError):
            date_of_filing = ""

        try:
            applicant_name = value["party_details"]["applicant_name"][0][
                "name"
            ]
        except (KeyError, IndexError, TypeError):
            applicant_name = ""

        try:
            respondant_name = value["party_details"]["respondant_name"][0][
                "name"
            ],
        except (KeyError, IndexError, TypeError):
            respondant_name = ""

        try:
            applicant_legal_representative_name = value[
                "legal_representative"
            ]["applicant_legal_representative_name"],
        except (KeyError, IndexError, TypeError):
            applicant_legal_representative_name = ""

        try:
            respondent_legal_representative_name = value[
                "legal_representative"
            ]["respondent_legal_representative_name"],
        except (KeyError, IndexError, TypeError):
            respondent_legal_representative_name = ""

        print(parse_api_response(all_view_data))
        # self.view_data.append(
        #     {
        #         "case_year": case_year,
        #         "case_type": case_type,
        #         "date_of_filing": date_of_filing,
        #         "applicant_name": applicant_name,
        #         "respondant_name": respondant_name,
        #         "applicant_legal_representative_name": applicant_legal_representative_name,
        #         "respondent_legal_representative_name": respondent_legal_representative_name,
        #         "first_hearing_details_hearing_date": first_hearing_details_hearing_date,
        #         "first_hearing_details_coram": first_hearing_details_coram,
        #         "first_hearing_details_stage_of_case": first_hearing_details_stage_of_case,
        #         "last_hearing_details_hearing_date": last_hearing_details_hearing_date,
        #         "last_hearing_details_coram": last_hearing_details_coram,
        #         "last_hearing_details_stage_of_case": last_hearing_details_stage_of_case,
        #         "next_hearing_details_hearing_date": next_hearing_details_hearing_date,
        #         "next_hearing_details_coram": next_hearing_details_coram,
        #         "next_hearing_details_stage_of_case": next_hearing_details_stage_of_case,
        #         "case_history_hearing_date": case_history_hearing_date,
        #         "case_history_stage_of_case": stage_case,
        #     }
        # )
        # print(self.view_data)


if __name__ == "__main__":
    app = Main()
    app.get_security_tokens()
    # app.get_main_data()
    app.get_view_data(9910131011772024)