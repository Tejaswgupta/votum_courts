import requests
from bs4 import BeautifulSoup

def search_by_case_no(case_no, bench, year, appeal):
    url = "https://itat.gov.in/judicial/casestatus"

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.8",
        "Cache-Control": "max-age=0",
        "Content-Type": "application/x-www-form-urlencoded",
        "Sec-Ch-Ua": "\"Brave\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"macOS\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Sec-Gpc": "1",
        "Upgrade-Insecure-Requests": "1",
        "Cookie": "csrf_cookie_name=3d8d03f6a041d398dc6fb505003d82fe; ci_session=31ac4cb6959e25316479b37353a4851b7cf2dde2",
        "Referer": "https://itat.gov.in/judicial/casestatus",
        "Referrer-Policy": "strict-origin-when-cross-origin"
    }

    data = {
        "csrf_test_name": "3d8d03f6a041d398dc6fb505003d82fe",
        "csrf_test_name": "3d8d03f6a041d398dc6fb505003d82fe",
        "cvr1": "false",
        "lqc": "1",
        "bench1": bench,  # "201",
        "appeal_type1": appeal,  # "Income Tax Appeal|ITA",
        "numbernew": case_no,  # "1",
        "yearnew": year,  # "2024",
        "userCaptcha1": "FK2XWU",
        "btnSubmit1": "submit1"
    }

    response = requests.post(url, headers=headers, data=data)
    
    print(response.headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text)
        table = soup.find_all('table',class_='searchtble')
        print(table)
        
    else:
        print(f"Error: {response.status_code}")


def get_case_details(uid):
    url = "https://itat.gov.in/judicial/judicialdetail"

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.6",
        "Cache-Control": "max-age=0",
        "Content-Type": "application/x-www-form-urlencoded",
        "Sec-Ch-Ua": "\"Chromium\";v=\"124\", \"Brave\";v=\"124\", \"Not-A.Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"macOS\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Sec-Gpc": "1",
        "Upgrade-Insecure-Requests": "1",
        "Cookie": "csrf_cookie_name=367c793aafc5a4a593caa47fa7c21f48; ci_session=1db71170b0444df6919c9d271c848cd35b02d45d",
        "Referer": "https://itat.gov.in/judicial/casestatus",
        "Referrer-Policy": "strict-origin-when-cross-origin"
    }

    data = {
        "csrf_test_name": "367c793aafc5a4a593caa47fa7c21f48",
        "btnDetail": uid
    }

    response = requests.post(url, headers=headers, data=data)

    print(response.text)
    
    
if __name__ == "__main__":
    a = search_by_case_no(1,203,2024,'Income Tax Appeal|ITA')
    print(a)