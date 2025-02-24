# Car-related Post Crawler (AWS Lambda & S3)

## 코드 개요
이 프로젝트는 클리앙(Clien) 자동차 게시판에서 특정 키워드를 포함한 게시물을 크롤링하여 AWS S3에 저장하는 Lambda 함수입니다. 

## 주요 코드 설명

### `fetch_html(url: str) -> str`
- 주어진 URL에서 HTML을 가져옴
- User-Agent를 랜덤으로 변경하여 요청 수행
- 최대 `TRIAL_LIMIT`번 재시도

### `get_list_of_post_url(start_datetime, end_datetime, page_number_list, keyword)`
- 특정 키워드와 날짜 범위에 해당하는 게시물 URL 목록을 가져옴
- 게시물 필터링 후 유효한 게시물의 URL을 반환

### `save_post_to_s3(id, url, data_for_load_s3)`
- 게시물 데이터를 JSON으로 변환 후 AWS S3에 저장

### `lambda_handler(event, context) -> dict`
- AWS Lambda에서 실행되는 메인 함수
- 크롤링 실행 후 결과를 JSON 형식으로 반환

## Lambda 핸들러 입력값
Lambda에 전달할 `event` 형식:
```json
{
  "car_id": "ev3",
  "keywords": ["ev3", "ev4", "아토3"],
  "date": "2025-02-25",
  "batch": "1",
  "start_datetime": "2025-02-20T00:00:00",
  "end_datetime": "2025-02-25T23:59:59",
  "bucket": "your-s3-bucket-name"
}
```

## Lambda 핸들러 반환값
Lambda 실행 후 반환되는 응답 형식:
```json
{
  "statusCode": 200,
  "body": {
    "success": true,
    "end_time": "2025-02-25T14:30:00",
    "duration": 120.5,
    "car_id": "hyundai-ev",
    "date": "2025-02-25",
    "batch": "1",
    "start_datetime": "2025-02-20T00:00:00",
    "end_datetime": "2025-02-25T23:59:59",
    "attempted_posts_count": 100,
    "extracted_posts_count": 90
  }
}
```
- `success`: 실행 성공 여부
- `end_time`: 크롤링 종료 시간
- `duration`: 실행 시간 (초)
- `attempted_posts_count`: 시도한 게시물 개수
- `extracted_posts_count`: 성공적으로 저장된 게시물 개수

