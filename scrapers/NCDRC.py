import logging
import re
import xml.etree.ElementTree as ET
from urllib import parse

import ddddocr
import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ocr = ddddocr.DdddOcr(show_ad=False)
session = requests.session()
session.verify = False

CAPTCHA_URL = 'https://cms.nic.in/ncdrcusersWeb/Captcha.jpg'
CSRF_TOKEN_URL = 'https://cms.nic.in/ncdrcusersWeb/login.do?method=caseStatus'
DATA_URL = 'https://cms.nic.in/ncdrcusersWeb/login.do'
SIMPLE_URL = 'https://cms.nic.in/ncdrcusersWeb/servlet/util.GetCaseStatus'

SIMPLE_TAGS = '''
Case No.
Filing Date
Filed In
Complainant
Respondent
Next Hearing
Case Stage
Attached or Lower Court Case(s)
Application(s) Filed
Date Of Destruction
RBT Details
Higher Commission Case'''.split('\n')

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://cms.nic.in',
    'Connection': 'keep-alive',
    'Referer': 'https://cms.nic.in/ncdrcusersWeb/login.do?method=caseStatus',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Priority': 'u=1'
}

def get_csrf_token():
    response = session.get(CSRF_TOKEN_URL).text
    csrf_token = response.split('name="CSRFTOKEN" value="')[1].split('"')[0]
    return csrf_token

def table_to_list(soup):
    table = []
    for row in soup.find_all('tr'):
        row_data = []
        for cell in row.find_all(['td', 'th']):
            row_data.append(cell.get_text().strip())
        if row_data:
            table.append(row_data)
    data = []
    for row in table[1:-1]:
        data.append(dict(zip(table[0], row)))
    return [{
        'cino': item['Case No'],
        'date_of_decision': item['Date of Filing'],
        'pet_name': item['Complainant'],
        'res_name': item['Respondent'],
        'type_name': None
    } for item in data]

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def ncdrc_simple_search(state_code, dist_code, fano):
    try:
        csrf_token = get_csrf_token()
        print('Fetching Captcha...')
        captcha_image = session.get(CAPTCHA_URL).content
        print('Captcha fetched. Processing...')
        captcha_result = ocr.classification(captcha_image)
        print('Captcha: ', captcha_result)

        print('Start fetching data...')
        if state_code == '0':
            login_type = 'B'
        elif state_code != '0' and dist_code == '0':
            login_type = 'C'
        else:
            login_type = 'E'
        data = {
            'CSRFTOKEN': csrf_token,
            'caseidin': f'{state_code}/{dist_code}/{fano}',
            'stateCode': state_code,
            'distCode': dist_code,
            'fano': fano,
            'loginType': login_type,
            'captcha': captcha_result,
            'method': 'GetCaseStatus',
        }
        data = parse.urlencode(data)
        resp = session.post(SIMPLE_URL, data=data, headers=headers)
        root = ET.fromstring(resp.text)
        result = []

        for detail in root.findall('DETAIL'):
            id = detail.find('ID').text
            desc = detail.find('Desc').text.replace('<![CDATA[', '').replace(']]>', '')  # Remove CDATA tags
            desc = re.sub(r'<.*?>', '', desc)  # Remove HTML tags using regex
            result.append({'ID': id, 'Name': SIMPLE_TAGS[int(id)], 'Desc': desc})
        
        if result[0]['ID'] == '0':
            if result[0]['Desc'] == 'Captcha Mismatch':
                print('Captcha incorrect. Retrying...')
                return ncdrc_simple_search(state_code, dist_code, fano)
            else:
                print('Data fetch failed.')
                return

        print('Data fetched successfully.')

        mapped_json = {
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
            "petitioner_advocates": None,
            "respondent_advocates": None,
            "judges": None,
            "bench_name": None,
            "court_name": None,
            "history": None,
            "acts": None,
            "orders": None,
            "additional_info": None,
            "original_json": None
        }
        for item in result:
            if item['Name'] == 'Case No.':
                mapped_json['case_no'] = item['Desc']
            elif item['Name'] == 'Filing Date':
                mapped_json['filing_date'] = item['Desc']
            elif item['Name'] == 'Complainant':
                mapped_json['pet_name'] = item['Desc']
            elif item['Name'] == 'Respondent':
                mapped_json['res_name'] = item['Desc']
            elif item['Name'] == 'Next Hearing':
                mapped_json['next_listing_date'] = item['Desc']
        return mapped_json
    
    except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
 
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)       
def ncdrc_search(state_code, dist_code, date_from, date_to, case_number=None, complainant=None, respondent=None, advocate_complainant=None, advocate_respondent=None, case_type=None, category=None, andor='and'):
    try:
        if state_code == '0':
            login_type = 'B'
        elif state_code != '0' and dist_code == '0':
            login_type = 'C'
        else:
            login_type = 'E'
        
        condition_dict = {
            'condition': '0',
            'searchTxtHid': '',
            'searchTxt': '',
            'ctId': '0',
            'catId': '0',
        }

        csrf_token = get_csrf_token()
        print('Fetching Captcha...')
        captcha_image = session.get(CAPTCHA_URL).content
        print('Captcha fetched. Processing...')
        captcha_result = ocr.classification(captcha_image)
        print('Captcha: ', captcha_result)

        print('Start fetching data...')
        data = {
            'CSRFTOKEN': csrf_token,
            'method': 'loadMainNcdrcQryPg',
            'stateCode': state_code,
            'login': '',
            'distCode': dist_code,
            'loginType': login_type,
            'stateName': '',
            'districtName': '',
            'cid': '',
            'userType': login_type,
            'ncdrc_id': 'ncdrc',
            'state_id': state_code,
            'state_idD': '0',
            'dist_id': dist_code,
            'captchaText': captcha_result,
            'fano': '',
            'dtFrom': date_from,
            'dtTo': date_to,
            'andor': 'and',
        }
        data = parse.urlencode(data)

        case_number_dict = condition_dict.copy()
        if case_number:
            case_number_dict['condition'] = '1'
            case_number_dict['searchTxtHid'] = case_number
            case_number_dict['searchTxt'] = case_number
        data += '&' + parse.urlencode(case_number_dict)
        
        complainant_dict = condition_dict.copy()
        if complainant:
            complainant_dict['condition'] = '2'
            complainant_dict['searchTxtHid'] = complainant
            complainant_dict['searchTxt'] = complainant
        data += '&' + parse.urlencode(complainant_dict)

        respondent_dict = condition_dict.copy()
        if respondent:
            respondent_dict['condition'] = '3'
            respondent_dict['searchTxtHid'] = respondent
            respondent_dict['searchTxt'] = respondent
        data += '&' + parse.urlencode(respondent_dict)

        advocate_complainant_dict = condition_dict.copy()
        if advocate_complainant:
            advocate_complainant_dict['condition'] = '4'
            advocate_complainant_dict['searchTxtHid'] = advocate_complainant
            advocate_complainant_dict['searchTxt'] = advocate_complainant
        data += '&' + parse.urlencode(advocate_complainant_dict)

        advocate_respondent_dict = condition_dict.copy()
        if advocate_respondent:
            advocate_respondent_dict['condition'] = '5'
            advocate_respondent_dict['searchTxtHid'] = advocate_respondent
            advocate_respondent_dict['searchTxt'] = advocate_respondent
        data += '&' + parse.urlencode(advocate_respondent_dict)

        case_type_dict = condition_dict.copy()
        if case_type:
            case_type_dict['condition'] = '6'
            case_type_dict['searchTxtHid'] = case_type
            case_type_dict['ctId'] = case_type
        data += '&' + parse.urlencode(case_type_dict)

        category_dict = condition_dict.copy()
        if category:
            category_dict['condition'] = '7'
            category_dict['searchTxtHid'] = category
            category_dict['catId'] = category
        data += '&' + parse.urlencode(category_dict)

        resp = session.post(DATA_URL, data=data, headers=headers)
        if 'Incorret Captcha.' in resp:
            print('Captcha incorrect. Retrying...')
            return ncdrc_search(state_code, dist_code, date_from, date_to, case_number, complainant, respondent, advocate_complainant, advocate_respondent, case_type, category, andor)
        soup = BeautifulSoup(resp.text, 'html.parser')
        if soup.title.string == 'Query Report':
            print('Data fetched successfully.')
            table_soup = soup.find('table')
            table = table_to_list(table_soup)
            return table
        if 'Acknowledgment' in soup.title.string:
            print('Data not found.')
            return []
        print('Data fetch failed.')
        return []
    except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
if __name__ == '__main__':
    pass
    print(ncdrc_simple_search('0', '0', 'IA/8867/2024'))
    print(ncdrc_search('0', '0', '01/01/2023', '01/01/2024', case_type='1')[0])
