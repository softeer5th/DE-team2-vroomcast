# AWS Lambda 기반 Redshift 트렌드 분석 및 Slack 알림 시스템

AWS Lambda에서 실행되며, Redshift에서 데이터를 가져와 트렌드를 분석하고, 이상 징후를 감지하면 Slack으로 알림을 보내는 기능을 수행합니다.

## 주요 기능
- **Redshift에서 트렌드 데이터 추출**
- **중복 제거 및 테이블 갱신**
- **트렌드 변화량 분석 및 이상 탐지**
- **Slack 알림 전송**

## 환경 변수 설정
이 코드를 실행하기 위해 아래 환경 변수를 설정해야 합니다:
- `SECRET_ID`: Redshift 접속을 위한 AWS Secrets Manager의 ID
- `SLACK_WEBHOOK_URL`: Slack 알림을 전송할 웹훅 URL
- `SUPERSET_URL`: 분석 대시보드를 확인할 수 있는 Superset URL

## 함수 설명

### 1. `create_table_as_select()`
이 함수는 `v_total_trend` 테이블에서 중복 데이터를 제거한 후 새로운 테이블을 생성합니다.

#### **쿼리 설명**
```sql
CREATE TABLE public.v_total_trend_dup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY batch_time, car_id, category ORDER BY total_view DESC) AS row_number
    FROM public.v_total_trend
) t
WHERE row_number = 1;
```
- `ROW_NUMBER()` 윈도우 함수를 사용하여 `batch_time`, `car_id`, `category`를 기준으로 중복된 행 중에서 `total_view`가 가장 높은 데이터를 유지합니다.

이후 기존 테이블을 삭제하고 새로운 테이블의 이름을 변경하여 중복이 제거된 `v_total_trend` 테이블을 완성합니다.

---

### 2. `get_recent_half_day_trend()`
최근 6시간 동안의 트렌드 데이터를 Redshift에서 조회하여 `v_total_trend` 테이블에 저장하는 함수입니다.

#### **쿼리 설명**
```sql
WITH RECURSIVE TimeBuckets(bucket_end, window_start) AS (
    SELECT
        MAX(extracted_at) AS bucket_end,
        MAX(extracted_at) - INTERVAL '6 hours' AS window_start
    FROM v_post_dynamic
    UNION ALL
    SELECT
        bucket_end - INTERVAL '30 minutes',
        window_start
    FROM TimeBuckets
    WHERE bucket_end - INTERVAL '30 minutes' >= window_start
)
```
- `WITH RECURSIVE`를 사용하여 6시간 동안 30분 간격으로 시간 구간을 생성합니다.

```sql
TargetPosts AS (
    SELECT
        pc.car_id,
        ps.id AS post_id
    FROM post_static ps
    JOIN post_car pc ON ps.id = pc.post_id
)
```
- `post_static`과 `post_car`를 조인하여 `car_id`와 해당 차량과 관련된 `post_id`를 추출합니다.

```sql
CategoryPosts AS (
    WITH PostCategoryCount AS (
        SELECT
            s.source_id AS post_id,
            kc.category AS category,
            COUNT(kc.category) AS category_count
        FROM sentence s
        JOIN keyword_category kc ON s.id = kc.sentence_id
        WHERE s.from_post = TRUE
        GROUP BY s.source_id, kc.category
    )
```
- `sentence`와 `keyword_category` 테이블을 활용하여 각 게시글(post)에 속한 카테고리(category) 빈도를 계산합니다.

```sql
SELECT
    tb.bucket_end AS batch_time,
    cp.car_id,
    cp.category,
    COALESCE(AVG(pd.v_view_count), 0) AS total_view,
    COALESCE(AVG(pd.v_comment_count), 0) AS total_comment,
    COALESCE(AVG(pd.v_upvote_count), 0) AS total_upvote,
    COALESCE(AVG(pd.v_downvote_count), 0) AS total_downvote
FROM TimeBuckets tb
JOIN CategoryPosts cp ON TRUE
LEFT JOIN v_post_dynamic pd
    ON pd.extracted_at > tb.bucket_end - INTERVAL '3 hours'
    AND pd.extracted_at <= tb.bucket_end
    AND pd.id = cp.post_id
GROUP BY tb.bucket_end, cp.car_id, cp.category
ORDER BY tb.bucket_end, cp.car_id, cp.category;
```
- `TimeBuckets`에서 생성한 30분 간격의 시간 구간을 기준으로 `v_post_dynamic` 테이블에서 `extracted_at`이 해당 시간 구간 내에 있는 데이터를 조회합니다.
- `COALESCE()`를 사용하여 NULL 값을 0으로 처리합니다.
- `GROUP BY`를 사용하여 차량별(`car_id`), 카테고리별(`category`)로 평균 조회수(`total_view`), 댓글 수(`total_comment`) 등을 집계합니다.

해당 데이터를 `v_total_trend` 테이블에 저장합니다.

---

