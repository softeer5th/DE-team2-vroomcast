from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
def _get_driver():
    # 이 path는 로컬 실행 시 주석처리 하세요.
    # chrome_path = "/chrome-headless-shell-mac-arm64"
    # driver_path = "/chromedriver-mac-arm64"   

    options = webdriver.ChromeOptions()
    # options.binary_location = chrome_path  # Chrome 실행 파일 지정 (로컬 실행 시 주석 처리)
    options.add_argument("--headless")  # Headless 모드
    options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    # options.add_argument("--disable-dev-tools")
    # options.add_argument("user-agent=Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0")
    options.add_argument("--window-size=1920x1080")
    # 아래 두 행은 로컬 실행 시 주석처리
    # options.binary_location = "/opt/chrome/chrome-linux64/chrome"
    # service = Service(executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver")
    
    driver = webdriver.Chrome(
        # service=service, # 로컬 실행 시 주석 처리
        options=options) 
    return driver


if __name__=="__main__":
    print("Starting Test ...")
    driver = _get_driver()
    print("driver has set.")
    if driver:
        # driver.get("https://namu.wiki/")
        # time.sleep(1)
        # soup = BeautifulSoup(driver.page_source, "html.parser")
        # door = soup.select_one("span.OXif9nG5").get_text()
        # print(door)
        
        driver.get("https://gall.dcinside.com/board/lists?id=car_new1&search_pos=-10358192&s_type=search_subject&s_keyword=.EC.8B.BC.ED.83.80.ED.8E.98")
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        gall_time = soup.select("tr.ub-content.us-post")
        for post in gall_time:
            # 날짜 검증
            date = post.select_one("td.gall_date")['title'] if post.select_one("td.gall_date") else "날짜 없음"        
            print(date)
        
        print("Test Successfully Ended")
        driver.quit()
    else:
        print("Something Wrong in Code")