from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
def _get_driver():
    # 이 path는 로컬 실행 시 주석처리 하세요.
    chrome_path = "/opt/chrome/chrome-headless-shell-linux64"
    # driver_path = "/opt/chromedriver"   

    options = webdriver.ChromeOptions()
    # options.binary_location = chrome_path  # Chrome 실행 파일 지정 (로컬 실행 시 주석 처리)
    options.add_argument("--headless")  # Headless 모드
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    # options.add_argument("--disable-dev-tools")
    # options.add_argument("user-agent=Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0")
    options.add_argument("--window-size=1920x1080")
    options.binary_location = "/opt/chrome/chrome-linux64/chrome"
    service = Service(executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver")
    # service = Service(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(
        service=service, # 로컬 실행 시 주석 처리
        options=options) 
    return driver

if __name__=="__main__":
    driver = _get_driver()