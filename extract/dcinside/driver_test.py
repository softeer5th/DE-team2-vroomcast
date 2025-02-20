from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from tempfile import mkdtemp

def get_driver():
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    chrome_options.add_argument(f"--data-path={mkdtemp()}")
    chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    chrome_options.add_argument("--log-path=/tmp")
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"
    prefs = {
        "profile.managed_default_content_settings.images": 2,  # 이미지 비활성화
        "profile.managed_default_content_settings.ads": 2,     # 광고 비활성화
        "profile.managed_default_content_settings.media": 2    # 비디오, 오디오 비활성화
    }
    chrome_options.add_experimental_option("prefs", prefs)

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_log_path="/tmp/chromedriver.log"
    )

    driver = Chrome(
        service=service, # 도커 환경에서 사용시 주석 해제하세요.
        options=chrome_options
    )

    return driver
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# import time
# from bs4 import BeautifulSoup
# from webdriver_manager.chrome import ChromeDriverManager
# def get_driver():

#     options = webdriver.ChromeOptions()
#     # options.binary_location = chrome_path  # Chrome 실행 파일 지정 (로컬 실행 시 주석 처리)
#     options.add_argument("--disable-extensions")
#     options.add_argument("--headless")  # Headless 모드
#     options.add_argument("--no-sandbox")
#     options.add_argument("--disable-dev-shm-usage")
#     options.add_argument("--disable-gpu")
#     options.add_argument("--disable-dev-tools")
#     options.add_argument("--window-size=1920x1080")
#     options.add_argument("user-agent=Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)")
#     # options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0Safari/537.36")
#     # options.add_argument("--remote-debugging-port=9222")
#     # 아래 두 행은 로컬 실행 시 주석처리
#     # options.binary_location = "/opt/chrome/chrome-headless-shell"
#     options.binary_location = "/opt/chrome/chrome-linux64/chrome"    
#     service = Service(executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver")
#     # service = Service(ChromeDriverManager().install())
    
    
#     attempt = 0
#     while attempt < (max_retries:=2):
#         try:
#             driver = webdriver.Chrome(service=service, options=options)
#             print("Chrome driver 생성에 성공했습니다.")
#             return driver
#         except Exception as e:
#             attempt += 1
#             print(f"Driver 생성 시도 {attempt}회차 실패: {e}")
#             time.sleep(2)  # 재시도 전 잠시 대기
#     raise Exception("여러 번 시도에도 webdriver.Chrome() 생성에 실패했습니다.")


if __name__=="__main__":
    print("Starting Test ...")
    try:
        driver = get_driver()
        print("Chrome driver has set.")
    except:

        print("driver hasn't set.")
    if driver:
        driver.get("https://www.google.com")
        print("Page title:", driver.title)
        driver.quit()        
        
        print("Test Successfully Ended")
        driver.quit()
    else:
        print("Something Wrong in Code")