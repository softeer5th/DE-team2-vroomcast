import json
import tempfile
import os
from unittest.mock import MagicMock
import pyarrow as pa
import pyarrow.parquet as pq
from main import (_split_data, POST_SCHEMA, COMMENT_SCHEMA,
                 POST_PATH, COMMENT_PATH)

# 테스트용 샘플 데이터
sample_data = {
    "post_id": 12345,
    "post_url": "https://example.com/post/12345",
    "title": "Test Post",
    "content": "This is a test post content",
    "created_at": "2024-02-14T12:00:00",
    "view_count": 100,
    "upvote_count": 10,
    "downvote_count": 2,
    "comment_count": 3,
    "comments": [
        {
            "comment_id": 1,
            "content": "Test comment 1",
            "is_reply": False,
            "created_at": "2024-02-14T12:30:00",
            "upvote_count": 5,
            "downvote_count": 1
        },
        {
            "comment_id": 2,
            "content": "Test comment 2",
            "is_reply": True,
            "created_at": "2024-02-14T12:35:00",
            "upvote_count": 3,
            "downvote_count": 0
        }
    ]
}

def test_data_conversion():
    car_id = "test_car"
    date = "2024-02-14"
    
    # 데이터 변환
    post, comments = _split_data(sample_data)
    
    # 저장할 파일 경로
    post_path = POST_PATH.format(car_id=car_id, date=date)
    comment_path = COMMENT_PATH.format(car_id=car_id, date=date)
    
    # 디렉토리 생성
    os.makedirs(os.path.dirname(post_path), exist_ok=True)
    os.makedirs(os.path.dirname(comment_path), exist_ok=True)
    
    # Parquet 파일로 저장
    posts_table = pa.Table.from_pylist([post], schema=POST_SCHEMA)
    comments_table = pa.Table.from_pylist(comments, schema=COMMENT_SCHEMA)
    
    pq.write_table(posts_table, post_path)
    pq.write_table(comments_table, comment_path)
    
    # 파일 생성 확인
    print("\n=== 파일 생성 확인 ===")
    print(f"Posts 파일 존재: {os.path.exists(post_path)}")
    print(f"Comments 파일 존재: {os.path.exists(comment_path)}")
    
    # 데이터 읽어서 확인
    if os.path.exists(post_path):
        print("\n=== Posts 데이터 ===")
        posts_table = pq.read_table(post_path)
        print(posts_table.to_pandas())
        print("\n=== Posts 스키마 ===")
        print(posts_table.schema)
        
    if os.path.exists(comment_path):
        print("\n=== Comments 데이터 ===")
        comments_table = pq.read_table(comment_path)
        print(comments_table.to_pandas())
        print("\n=== Comments 스키마 ===")
        print(comments_table.schema)

if __name__ == "__main__":
    test_data_conversion()