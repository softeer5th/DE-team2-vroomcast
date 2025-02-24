import awswrangler as wr
import json
import requests
import os
from datetime import datetime, timezone, timedelta

# Redshift 연결
conn = wr.redshift.connect(secret_id=os.getenv("SECRET_ID"))


def create_table_as_select():
    """v_total_trend 테이블의 중복 데이터를 제거한 새 테이블 생성 및 교체"""
    cursor = conn.cursor()
    create_query = """
        CREATE TABLE public.v_total_trend_dup AS
        SELECT batch_time, car_id, category, total_view, total_comment, total_upvote, total_downvote
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY batch_time, car_id, category ORDER BY total_view DESC) AS rn
            FROM public.v_total_trend
        ) t
        WHERE rn = 1;
    """
    cursor.execute(create_query)
    print("v_total_trend_dup 테이블 생성 완료!")

    # 기존 테이블 삭제 후, 새 테이블 이름 변경
    cursor.execute("DROP TABLE public.v_total_trend;")
    cursor.execute("ALTER TABLE public.v_total_trend_dup RENAME TO v_total_trend;")
    print("v_total_trend 테이블 교체 완료!")


def get_recent_half_day_trend():
    """최근 12시간 동안의 트랜드 데이터를 Redshift에서 읽어오고 v_total_trend 테이블에 저장"""
    QUERY = """
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
    ),
    TargetPosts AS (
        SELECT
            pc.car_id,
            ps.id AS post_id
        FROM post_static ps
        JOIN post_car pc ON ps.id = pc.post_id
    ),
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
        ),
        RankedCategories AS (
            SELECT
                post_id,
                category,
                ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY category_count DESC) AS rank
            FROM PostCategoryCount
        )
        SELECT
            tp.car_id,
            rc.post_id,
            rc.category
        FROM RankedCategories rc
        JOIN TargetPosts tp ON rc.post_id = tp.post_id
        WHERE rc.rank = 1
    )
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
    """
    df = wr.redshift.read_sql_query(sql=QUERY, con=conn)
    wr.redshift.to_sql(
        df=df,
        con=conn,
        schema="public",
        table="v_total_trend",
        mode="append",
        index=False,
    )
    return df

def format_slack_messages(alerts_df):
    """Slack 메시지를 경고 내역에 맞게 포맷팅"""
    now_utc = datetime.now(timezone.utc)  # UTC 현재 시간
    current_time = now_utc + timedelta(hours=9)
    slack_messages = []
    header_text = ":rotating_light: *조회 집계 경고* :rotating_light:"
    for _, row in alerts_df.iterrows():
        body_text = (
            f"*🕒 기준 시간:* {current_time:%Y-%m-%d %H:%M:%S}\n\n"
            f"🚗 *Car:* `{row['car_id']}`\n\n🏆 *Category:* `{row['category']}`\n\n"
            f"📈 *Change:* `{row['view_change']}`\n\n🔢 *Total:* `{row['total_view']}k`\n\n"
        )
        slack_message = {
            "text": ":eyes: 소셜 트랜드 이상징후 발견",
            "attachments": [
                {
                    "color": "#ff0000",
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": header_text,
                                "emoji": True
                            }
                        },
                        {"type": "divider"},
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": body_text
                                }
                            ]
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f":information_source: 이 메시지는 자동 알림 시스템에 의해 전송되었습니다.\n추가 정보는 {os.getenv('SUPERSET_URL')} 에서 확인하세요"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        slack_messages.append(slack_message)
    return slack_messages


def send_slack_notification(slack_messages, webhook_url: str):
    """Slack 웹훅 URL로 알림 전송"""
    if not slack_messages:
        return 200
    for slack_message in slack_messages:
        response = requests.post(
            webhook_url, data=json.dumps(slack_message),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            print(f"Slack 요청 실패: {response.status_code}, 응답:\n{response.text}")
        else:
            print("Slack 알림 전송 완료!")


def main():
    # 최근 12시간 트랜드 데이터 조회 및 테이블 업데이트
    trend_df = get_recent_half_day_trend()
    create_table_as_select()

    # 각 그룹의 조회수 변화량 계산 (이전 값과의 차이)
    trend_df['view_change'] = trend_df.groupby(['car_id', 'category'])['total_view'].diff()

    # 전체 데이터 기준 threshold 계산: 평균 + 2 * 표준편차
    overall_mean = trend_df['view_change'].mean()
    overall_std = trend_df['view_change'].std()
    threshold = overall_mean + 2 * overall_std
    print(f"전체 변화량 - 평균: {overall_mean:.2f}, 표준편차: {overall_std:.2f}, 임계치: {threshold:.2f}")

    # 최신 배치 시간의 데이터만 필터링
    latest_time = trend_df['batch_time'].max()
    latest_df = trend_df[trend_df['batch_time'] == latest_time].copy()

    # 최신 데이터에서 각 car_id, category별 최대 view_change와 total_view 계산
    grouped = latest_df.groupby(['car_id', 'category'], as_index=False).agg({
        'view_change': 'max',
        'total_view': 'max'
    })
    print(grouped)

    # threshold를 초과하는 alert만 필터링
    alert_df = grouped[grouped['view_change'] > threshold]
    for _, row in alert_df.iterrows():
        print(
            f"Alert - car_id: {row['car_id']}, category: {row['category']}, view change: {row['view_change']:.2f}, total_view: {row['total_view']}")

    # Slack 웹훅 URL (환경변수에서 불러옴)
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    # Slack 메시지 포맷 및 전송
    slack_messages = format_slack_messages(alert_df)
    send_slack_notification(slack_messages, webhook_url)

    return {
        'statusCode': 200,
        'body': json.dumps({"message": "Notification sent successfully!"})
    }


def lambda_handler(event, context):
    try:
        return main()
    except Exception as e:
        print(f"Error occurred: {e}")