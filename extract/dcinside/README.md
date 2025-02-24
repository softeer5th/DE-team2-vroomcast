# 디시인사이드 크롤러
디시인사이드 자동차 갤러리의 게시물 내용을 크롤링하여 결과물을 json파일로 변환 후 S3 데이터베이스로 저장

## Install Requirements
- beautifulsoup4 >= 4.13.3
- requests >= 2.32.3
- selenium >= 4.28.1
  - ⚠️ 4.9를 넘어가는 버전은 예기치 않은 오류가 발생 가능

## Files
- **main.py**
    - AWS Lambda에서 lambda_handler 함수를 통해 실행되는 파일
    - DC인사이드 자동차 갤러리에서 원하는 검색어의 결과 중 특정 시간 범위의 글을 크롤링 후 저장
    - Output: `extracted/{self.car_id}/{self.folder_date}/{self.batch}/raw/dcinside/{post_id}.json` 경로 및 파일명으로 S3에 저장됨
    - lambda event 예시:
        ``` python
        
        { 
            'bucket' : "S3_bucket_name",
            'car_id' : "santafe",
            'keywords': ["산타페", "싼타페"],
            'date': "2025-02-20",
            'batch': 0,
            'start_datetime': "2024-05-07T00:00:00",
            'end_datetime' : "2024-05-30T00:00:00"
        }
        ```
<br>

- **driver_test.py**
    - main.py를 실행해 보기 전, 컨테이너 환경에서 Chrome driver가 정상적으로 작동하는지 확인하는 용도
    - 각종 옵션을 실험해 볼 수 있음
    - Output: (정상 작동 시)
        ```bash
            Starting Test ...
            Chrome driver has set.
            Page title: Google
            Test Successfully Ended
        ```
<br>

- **Dockerfile**
    - aws-lambda용 파이썬 3.12 이미지 기반
    - 첨부된 chrome-installer.sh로 chrome, chrome-driver 다운로드
<br>

- **chrome-installer.sh**
    - chrome, chrome-driver의 최신 stable version을 다운로드

## Notes
- 검색 시작, 끝 시간의 Datetime의 형식은 `YYYY-mm-DDTHH:MM:SS`입니다.
- Lambda에서 구동 시 2048MB 이상의 메모리를 필요로 합니다.
