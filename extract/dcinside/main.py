from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from datetime import datetime, timedelta
from dateutil import parser
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time, json, logging, requests, os
from bs4 import BeautifulSoup
import boto3, random

logging.basicConfig(level=logging.INFO)  # 로그 레벨 설정
logger = logging.getLogger(__name__)


BASE_URL = "https://gall.dcinside.com/board/lists/?id=car_new1"
WAIT_TIME = 2

# 제목만 / 제목+내용
SEARCH_URL_TITLE = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject&s_keyword="
SEARCH_URL_TITLE_AND_CONTENT = f"https://gall.dcinside.com/board/lists/?id=car_new1&s_type=search_subject_memo&s_keyword="  

def aws_lambda_logging_ok(log_text):
    return {
        'statusCode': 200,
        'body': log_text#'✅ File uploaded successfully'
    }
    
def aws_lambda_logging_fail(log_text):
    return {
        'statusCode': 500,
        'body': log_text#'✅ File uploaded successfully'
    }  
    
def convert_date_format(date_str):
    # date_str = str(md_to_ymd(date_str))
    return str(date_str).split(' ')[0].replace('.', '-')

def md_to_ymd(date_str:str):
    """
    두 가지 날짜 형식을 입력받아 "yyyy.mm.dd HH:MM:SS" 형식으로 변환합니다.
    본문 및 댓글의 날짜 형식에 대응합니다.
    
    Args:
        date_str: 변환할 날짜 문자열 ("yyyy.mm.dd HH:MM:SS" 또는 "mm.dd HH:MM:SS" 형식)

    Returns:
        "yyyy.mm.dd HH:MM:SS" 형식으로 변환된 날짜 문자열
    """
    try:
        # "yyyy.mm.dd HH:MM:SS" 형식인 경우 그대로 반환
        datetime.strptime(date_str, "%Y.%m.%d %H:%M:%S")
        return date_str
    except ValueError:
        try:
            # "mm.dd HH:MM:SS" 형식인 경우 연도를 2025로 가정하여 변환
            date_obj = datetime.strptime(date_str, "%m.%d %H:%M:%S")
            return date_obj.replace(year=datetime.now().year).strftime("%Y.%m.%d %H:%M:%S")
        except ValueError:
            return "Invalid date format"

def date_type_for_search_result(date_str):
    try:
        # 'mm.dd' 형식 처리
        date_obj = datetime.strptime(date_str, "%m.%d")
        date_obj = date_obj.replace(year=datetime.now().year)
        # print(date_obj)
        return date_obj
    except ValueError:
        try:
            # 'yy.mm.dd' 형식 처리
            date_obj = datetime.strptime(date_str, "%y.%m.%d")
            # print(date_obj)
            return(date_obj)
        except ValueError:
            return "Invalid Date Format"

def is_date_in_range(date, start_date_str, end_date_str):
    """
    주어진 날짜 문자열이 특정 날짜 범위 안에 있는지 확인합니다 (dateutil 사용).
    검색 결과 목록의 날짜 형식에 대응합니다.
    
    Args:
        date_str: 검사할 날짜 문자열 (예: '23.08.17' | '02.15')
        start_date_str: 시작 날짜 문자열 (예: '2023-08-16')
        end_date_str: 종료 날짜 문자열 (예: '2023-11-16')

    Returns:
        bool: 날짜가 범위 안에 있으면 True, 아니면 False
    """


    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        return start_date <= date <= end_date

    except ValueError:
        return "Invalid Date Format"
    
    
class DC_crawler:
    MAX_TRY = 2
    RETRY_WAITS = 2
    post_link = [
    ]
    
    def __init__(self, s_date, e_date, car_id, car_keyword, bucket_name):
        self.start_date = s_date
        self.end_date = e_date
        self.car_id = car_id
        self.keyword = car_keyword
        self.search_url = SEARCH_URL_TITLE + car_keyword
        self.BUCKET_NAME = bucket_name
        self.s3 = boto3.client("s3")
        
    # Chrome WebDriver 선언, Lambda 적용 시 주석 필히 보고 해제할 것!!!!!
    def _get_driver(self,):
        # 이 path는 로컬 실행 시 주석처리 하세요.
        # chrome_path = "/opt/chrome/chrome-headless-shell-mac-arm64"
        # driver_path = "/opt/chromedriver"   

        options = webdriver.ChromeOptions()
        
        options.add_argument("--headless")  # Headless 모드
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--single-process")
        # options.add_argument("user-agent=Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)")
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0")
        options.add_argument("--window-size=1420, 1080")
        options.add_argument('--blink-settings=imagesEnabled=false')    
        options.binary_location = "/opt/chrome/chrome-linux64/chrome" # Chrome 실행 파일 지정 (로컬 실행 시 주석 처리)
        service = Service(executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver")
        
        driver = webdriver.Chrome(
            service=service, # 로컬 실행 시 주석 처리
            options=options) 
        
        if driver:
            print("✅ Driver Successfully Set.")
            return driver
        else:
            print("❌ Driver Setting Failed.")
            return False
    
    def get_entry_point(self, driver:webdriver.Chrome, url):
        s_date = self.start_date
        e_date = self.end_date
        
        driver.get(url)
        time.sleep(WAIT_TIME)
        #-----------------------------------------------
        # 🔹 1. 날짜 검색 창을 여는 버튼 클릭
        #-----------------------------------------------
        open_date_picker = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn_grey_roundbg.btn_schmove")))
        open_date_picker.click()
        time.sleep(1)  # 검색 창이 뜨는 시간 고려
        
        #-----------------------------------------------
        # 🔹 2. 날짜 입력 필드 찾기
        #-----------------------------------------------
        date_input = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input.dayin.calendar")))
        
        #-----------------------------------------------
        # 🔹 3. 날짜 입력
        #-----------------------------------------------
        target_date = e_date  # 검색할 날짜
        # JavaScript로 날짜 값 변경
        driver.execute_script("arguments[0].value = arguments[1];", date_input, target_date)
        date_input.send_keys(target_date)
        date_input.send_keys(Keys.RETURN)  # 엔터 입력

        #-----------------------------------------------
        # 🔹 4. 검색 버튼 클릭
        #-----------------------------------------------
        search_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn_blue.small.fast_move_btn"))
        )
        search_btn.click()

        #-----------------------------------------------
        # 🔹 5. 검색 결과 로딩 대기
        #-----------------------------------------------
        time.sleep(0.5)  # 네트워크 환경에 따라 조정
        
        #-----------------------------------------------
        # 🔹 6. 페이지 소스 가져오기
        #-----------------------------------------------
        current_page_url = driver.current_url
        return current_page_url        
        
    def crawl_post_link(self, soup:BeautifulSoup, cur_date:str):
        """
        현재 페이지에서 게시글들의 링크를 수집합니다.
        """
        posts = soup.select("tr.ub-content.us-post")
        
        for post in posts:
            # 날짜 검증
            date = post.select_one("td.gall_date").get_text(strip=True) if post.select_one("td.gall_date") else "날짜 없음"
            date = date_type_for_search_result(date)
            
            if not is_date_in_range(date, self.start_date, self.end_date):
                logger.info(f"❗️ Stopped by found date {str(date).split()[0]}")
                print(f"❗️ Stopped by found date {str(date).split()[0]}")
                return False
            
            date = str(date).split()[0]
            
            # 날짜 넘어갈 시 로그 작성
            if date != cur_date:
                logger.info(f"🔗 of {date}")
                print(f"🔗 of {date}")
                cur_date = date
              
            gall_num = int(post.select_one("td.gall_num").get_text(strip=True))
            dc_url = "https://gall.dcinside.com"
            title_tag = post.select_one("td.gall_tit.ub-word a")
            link = dc_url + title_tag["href"] if title_tag else "링크 없음"
            
            post_info = {
                "url" : link,
                "id" : gall_num,
                "date" : date
            }
            
            self.post_link.append(post_info)
        return date
    
    def page_traveler(self, driver:webdriver.Chrome, current_link:str):
        """
        페이징 박스를 순회합니다. <br>
        시간 **역순**으로 순회합니다. <br>
        (페이징 박스는 정방향 순회, 보이는 게시글은 시간 역순)
        """
        # random_sleep_time = [0.8, 0.6, 0.7, 0.5]
        cur_date = self.end_date
        # i = 0
        
        while True:
            driver.get(current_link)
            time.sleep(WAIT_TIME - 1)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # is_crawl_post_success = False
            date = self.crawl_post_link(soup, cur_date)
            
            if date: # 유효하지 않은 날짜를 만날 때 까지 크롤링
                # 한 페이지를 다 긁었으면...
                current_page = soup.select_one('.bottom_paging_box.iconpaging em')
                dc_url = "https://gall.dcinside.com"
                next_link = current_page.find_next_sibling('a')
                current_link = dc_url + next_link['href']
                
                # if next_link.get('class') == "search_next": 
                #     logger.info("Search next 10000 posts")
                
                time.sleep(random.randrange(50, 100) / 100)
                # i += 1
            
                cur_date = date    
                
            else: # 특정 범위의 날짜를 전부 크롤링 했다면
                logger.info(f"✅ crawling {self.start_date} ~ {self.end_date} finished")
                print(f"✅ crawling {self.start_date} ~ {self.end_date} finished")
                break
        return
    
    def get_html_of_post(self, driver:webdriver.Chrome, url:str):
        """
        각 게시글의 html source를 가져옵니다.
        가져온 source를 반환합니다.
        """
        # headers = {'User-Agent': "Mozilla/5.0 (compatible; Daum/3.0; +http://cs.daum.net/)"}
        for _ in range(self.MAX_TRY):
            try:
                driver.get(url)
                time.sleep(WAIT_TIME - (random.randrange(50, 100) / 100))
                soup = BeautifulSoup(driver.page_source, "html.parser")
                if soup:
                    return soup
            
            except:# 페이지 접근 재시도
                logger.error(f"❌ {url} request FAILED!")
                print(f"❌ {url} request FAILED!")
                time.sleep(self.RETRY_WAITS)
                continue
        return False
            
    def html_parser(self, driver:webdriver.Chrome, post_info:dict, parsed_post:BeautifulSoup):
        print("Now Parsing ▶ " , driver.current_url)

        def parse_main_content(target_element):
            """
            게시글 본문 크롤링
            Returns:
                본문 내용, 추천 수, 비추 수
            """
            write_div = target_element.find("div", class_="write_div")
            gaechu = int(target_element.find("p", class_="up_num font_red").get_text(strip=True))
            bichu = int(target_element.find("p", class_="down_num").get_text(strip=True))
            content = write_div.get_text(separator="\n", strip=True)  # <br>을 \n으로 변환, 공백 제거
            return content, gaechu, bichu

        def parse_comments(soup:BeautifulSoup):
            """
            댓글 및 대댓글을 수집하여 리스트로 반환하는 함수.
            
            Args:
                soup (BeautifulSoup): BeautifulSoup으로 파싱된 HTML
            
            Returns:
                list[dict]: 댓글과 대댓글을 포함한 리스트
            """
            comment_list = []
            comment_ul = soup.find("ul", class_="cmt_list")
            
            if not comment_ul:
                return comment_list  # 댓글이 없으면 빈 리스트 반환

            for li in comment_ul.find_all("li", recursive=False):  # 최상위 li만 탐색 (대댓글 제외)
                # 🔹 댓글인지 대댓글인지 구분
                is_reply = 0  # 기본적으로 댓글(0)
                
                if "dory" in li.get("class", []): # 광고댓글 거르기 (댓글돌이 광고)
                    continue                      

                # 🔹 댓글 내용
                if (cmt_id := li.get('id')) and not li.select_one("p.del_reply"): # 댓글이면
                    content_tag = li.select_one("p.usertxt.ub-word")
                    content = content_tag.get_text(strip=True) if content_tag else ""

                    # 🔹 작성 시간 (datetime 변환)
                    created_at = li.select_one("span.date_time").get_text(strip=True) 
                    # isoformat으로 변환
                    ymd, hms = md_to_ymd(created_at).split()
                    created_at = 'T'.join([convert_date_format(ymd), hms])
                    # print(li.attrs["id"])
                    
                    comment_id = int(cmt_id.split('_')[-1])
                    
                    # 🔹 리스트에 추가
                    comment_list.append({
                        "comment_id": comment_id,
                        "content": content,
                        "is_reply": is_reply,
                        "created_at": created_at,
                        "upvote_count": 0,
                        "downvote_count": 0
                    })
                else:
                    comment_id = None

                if li.find("div", class_="reply_box"):
                    is_reply = 1  # 대댓글(1)
                # 🔹 대댓글 탐색
                reply_ul = li.select_one("ul.reply_list")
                
                if reply_ul:
                    reply_parent_id = int(reply_ul.get('id').split('_')[-1])
                    for reply_li in reply_ul.find_all("li", class_="ub-content"):
                        # reply_parent_id = comment_id
                        if reply_content_tag := reply_li.select_one("p.usertxt.ub-word"):
                            reply_content = reply_content_tag.get_text(strip=True) if reply_content_tag else ""

                            reply_created_at = reply_li.select_one("span.date_time").get_text(strip=True) 
                            ymd, hms = md_to_ymd(reply_created_at).split()
                            reply_created_at = 'T'.join([convert_date_format(ymd), hms])

                            comment_list.append({
                                "comment_id": reply_parent_id,
                                "content": reply_content,
                                "is_reply": 1,  # 대댓글
                                "created_at": reply_created_at,
                                "upvote_count": 0,
                                "downvote_count": 0                            
                            })
                        else: continue

            return comment_list

        def scrape_all_comment_pages(driver:webdriver.Chrome, soup:BeautifulSoup):
            """
            주어진 soup을 기반으로 댓글 페이지를 순회하며 모든 댓글을 수집하는 함수.
            """
            comment_count_tag = soup.find('span', class_='gall_comment')
            comment_count = int(comment_count_tag.find('a').text[len("댓글 "):]) if comment_count_tag else 0
            
            all_comments = []  # 모든 댓글을 저장할 리스트

            # 🔹 첫 번째 페이지 댓글 수집
            comments = parse_comments(soup)
            all_comments.extend(comments)
            

            # 🔹 다음 댓글 페이지 버튼 찾기
            paging_box = soup.select_one("div.cmt_paging")
            if not paging_box:
                # print("댓글 페이지네이션이 없음.")
                return comments, all_comments

            next_page_btns = paging_box.find_all("a", href=True)

            for btn in next_page_btns:
                page_number = btn.get_text(strip=True)
                if page_number.isdigit():
                    # print(f"이동 중: 댓글 페이지 {page_number}")

                    # 🔹 JavaScript 실행하여 댓글 페이지 이동
                    driver.execute_script(btn["href"])

                    # 🔹 새로운 페이지 HTML을 가져오기 위해 대기
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.cmt_paging"))
                    )

                    # 🔹 새로운 soup 업데이트 후 댓글 추가 수집
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    comments = parse_comments(soup)
                    all_comments.extend(comments)

            return comment_count, all_comments

        post_url = post_info['url']
        post_id = post_info['id']
        
        created_at = parsed_post.find("span", class_="gall_date").get_text(strip=True)
        ymd, hms = created_at.split()
        created_at = 'T'.join([convert_date_format(ymd), hms])
        
        title = parsed_post.find("span", class_="title_subject").get_text(strip=True)
        view_count = int(parsed_post.find("span", class_="gall_count").get_text(strip=True)[len("조회 "):])
        content, up_vote, down_vote = parse_main_content(parsed_post)
        comment_count, comment_list = scrape_all_comment_pages(driver, parsed_post)
        
        parsed_finally = {
            "post_id" : post_id ,
            "post_url" : post_url,
            "title" : title,
            "content" : content,
            "created_at" : created_at,
            "view_count" : view_count,
            "upvote_count" : up_vote,
            "downvote_count" : down_vote,
            "comment_count" : comment_count,
            "comments" : comment_list
        }
                
        return parsed_finally
  

    def save_json(self, parsed_json:json, post_info:dict):
        file_date = post_info['date'][5:]
        # post_date = str(md_to_ymd(post_info['date']))
        file_path = f"extracted/{self.car_id}/{file_date}/raw/dcinside/{post_info['id']}.json"
        directory = os.path.dirname(file_path)

        
        # if not os.path.exists(directory):  # 디렉토리가 존재하지 않으면
        #     os.makedirs(directory)  # 디렉토리 생성
        # 1. 폴더 존재 확인
        try:
            self.s3.head_object(Bucket=self.BUCKET_NAME, Key=directory)
            print("✅ S3 folder route exists")
        except:  # 폴더가 없는 경우
            print(f"❌ S3 folder route doesn't exists. Making directory...{directory}")
            
        try:
            # with open(file_path, "w", encoding="utf-8") as file:
                # file.write(html_source)
            web_data = json.dumps(parsed_json, ensure_ascii=False, indent=4)
            print(f"✅ Post ID: {post_info['id']} → File Created")
            
        except Exception as e:
            print(f"❌ json.dumps 중 오류 발생: {e}")       
            
        try:
            self.s3.put_object(
                Bucket = self.BUCKET_NAME,
                Key = file_path,
                Body = web_data,
                ContentType = "application/json"
            )     
            logger.info(f"✅ Successfully uploaded {post_info['id']}.json to s3-bucket")
            print(f"✅ Successfully uploaded {post_info['id']}.json to s3-bucket")

        except Exception as e:
            logger.error(f"❌ Error uploading file to S3: {e}", exc_info=True)
            print(f"❌ Error uploading file to S3: {e}", exc_info=True)

        
    def run_crawl(self,):
        # 드라이버 세팅
        driver=self._get_driver()
        logger.info("✅ Driver Successfully Set.")
        
        # 검색 기간 내 가장 최신 게시글 검색 결과 접근
        end_point = self.get_entry_point(driver, url=self.search_url)
        # if end_point:
        #     aws_lambda_logging_ok("✅ Successfully accessed to init date")
        # else:
        #     aws_lambda_logging_fail("❌ Failed to access init date")
            
        logger.info("✅ Successfully accessed to init date")
        print("✅ Successfully accessed to init date")
        # 접근 위치로부터 거슬러 올라가며 게시글 링크 수집
        self.page_traveler(driver, end_point)
        print(f"✅ Gathering link completed : {len(self.post_link)} links")
        
        # 수집된 링크를 방문하며 html 소스 저장
        for i, post in enumerate(self.post_link):
            # print(f"Progressing... [{i+1} / {len(self.post_link)}]")
            
            # random_sleep_time = [0.8, 0.6, 0.7, 0.5]
            parsed_source = self.get_html_of_post(driver, post['url'])
            res_json = self.html_parser(driver, post, parsed_source)
            
            logger.info(f"Saving...[{i+1} / {len(self.post_link)}]")
            print(f"Saving...[{i+1} / {len(self.post_link)}]")
            self.save_json(res_json, post)
                
            time.sleep(random.randrange(50, 100) / 100)
        
        driver.close()
        return True  
                      
def calculate_dates(a):
    """
    주어진 날짜 a를 기준으로 월을 변경하고, 45일 전후 날짜를 계산합니다.

    Args:
        a: "YYYY-MM-DD" 형식의 날짜 문자열

    Returns:
        딕셔너리: 
            - (검색 종료일이 주어진 경우)
                - month_minus_3: 월 -3인 날짜
                - month_minus_3_minus_45: 월 -3인 날짜로부터 45일 전
            
            - (차량 출시일이 주어진 경우)
                - month_plus_3: 월 +3인 날짜
                - minus_45: a로부터 45일 전
    """
    year, month, day = map(int, a.split('-'))

    # 월 변경 및 날짜 계산 함수
    def change_month(y, m, d, delta):
        new_month = (m + delta - 1) % 12 + 1  # 월 변경
        new_year = y + (m + delta - 1) // 12  # 년도 변경
        try:
            return datetime(new_year, new_month, d).strftime("%Y-%m-%d")
        except ValueError:  # 월의 마지막 날짜 처리
            import calendar
            last_day = calendar.monthrange(new_year, new_month)[1]
            return datetime(new_year, new_month, last_day).strftime("%Y-%m-%d")

    # 결과 계산
    month_minus_3 = change_month(year, month, day, -3)
    month_minus_3_minus_45 = (datetime.strptime(month_minus_3, "%Y-%m-%d") - timedelta(days=45)).strftime("%Y-%m-%d")
    month_plus_3 = change_month(year, month, day, 3)
    # month_plus_3_minus_45 = (datetime.strptime(month_plus_3, "%Y-%m-%d") - timedelta(days=45)).strftime("%Y-%m-%d")
    minus_45 = (datetime.strptime(a, "%Y-%m-%d") - timedelta(days=45)).strftime("%Y-%m-%d")

    return {
        "month_minus_3": month_minus_3,
        "month_minus_3_minus_45": month_minus_3_minus_45,
        "month_plus_3": month_plus_3,
        # "month_plus_3_minus_45": month_plus_3_minus_45,
        "minus_45": minus_45
    }
    
def lambda_handler(event, context):
    BUCKET_NAME = event.get('bucket')
    car_id      = event.get('car_id') # santafe
    car_keyword = event.get('keywords') # 싼타페
    date        = event.get('date') # 2025-02-10
    release_date = event.get('release_date')
    test_date   = event.get('test_date')
    
    dates = calculate_dates(date)
    
    # 검색 구간의 끝 부분(현재와 가까운 쪽)이 주어질 경우
    s_date = dates['month_minus_3_minus_45']
    e_date = date
    
    if release_date:# 차량 출시일이 주어질 경우
        s_date = dates['minus_45']
        e_date = dates['month_plus_3']
    
    elif test_date:
        s_date = test_date[0]
        e_date = test_date[1]
    
    
    logger.info(f"✅ Initiating Crawler : {s_date} ~ {e_date}")
    print(f"✅ Initiating Crawler : {s_date} ~ {e_date}")
    # car_keyword는 lambda_handler에서 event로 처리하게 할 것
    crawler = DC_crawler(s_date, e_date, car_id=car_id, car_keyword=car_keyword, bucket_name=BUCKET_NAME)
    
    print("Running crawler")
    logger.info("Running crawler")
    if crawler.run_crawl():
        logger.info("✅ Crawling Finished")
        print("✅ Crawling Finished")
    else:
        logger.info("❌ Crawling Not Finished With Errors")
        print("❌ Crawling Not Finished With Errors")
    