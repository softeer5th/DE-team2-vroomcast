from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
def get_driver():

    options = webdriver.ChromeOptions()
    # options.binary_location = chrome_path  # Chrome 실행 파일 지정 (로컬 실행 시 주석 처리)
    options.add_argument("--disable-extensions")
    options.add_argument("--headless=new")  # Headless 모드
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-tools")
    options.add_argument("--window-size=1920x1080")
    # options.add_argument("user-agent=Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)")
    # options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0Safari/537.36")
    # options.add_argument("--remote-debugging-port=9222")
    # 아래 두 행은 로컬 실행 시 주석처리
    # options.binary_location = "/opt/chrome/chrome-headless-shell"
    options.binary_location = "/opt/chrome/chrome-linux64/chrome"    
    service = Service(executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver")
    # service = Service(ChromeDriverManager().install())
    
    
    try:
        driver = webdriver.Chrome(
        service=service, # 로컬 실행 시 주석 처리
        options=options) 
        print("Current session is {}".format(driver.session_id))
    except Exception as e:
        print(f"ERROR: {e}")
        # driver.quit()
    return driver


if __name__=="__main__":
    print("Starting Test ...")
    driver = get_driver()
    print("driver has set.")
    if driver:
        driver.get("https://www.google.com")
        print("Page title:", driver.title)
        driver.quit()        
        
        print("Test Successfully Ended")
        driver.quit()
    else:
        print("Something Wrong in Code")