import glob
import json
import os
from datetime import datetime
from typing import Dict, List

import pandas as pd


def read_json_files(extracted_path: str) -> List[Dict]:
    all_data = []
    json_files = glob.glob(os.path.join(extracted_path, "*.json"))

    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                all_data.append(data)
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            continue

    return all_data


def process_data(extracted_data: List[Dict]) -> tuple:
    posts_data = []
    comments_data = []

    for item in extracted_data:
        # Posts 데이터 추출
        post = {
            "post_id": item["post_id"],
            "post_url": item["post_url"],
            "title": item["title"],
            "content": item["content"],
            "author": item["author"],
            "created_at": pd.to_datetime(item["created_at"]),
            "view_count": item["view_count"],
            "upvote_count": item["upvote_count"],
            "downvote_count": item["downvote_count"],
            "comment_count": item["comment_count"],
        }
        posts_data.append(post)

        # Comments 데이터 추출
        for comment in item["comments"]:
            comment_data = {
                "comment_id": str(comment["comment_id"]),
                "post_id": item["post_id"],
                "content": comment["content"],
                "is_reply": comment["is_reply"],
                "author": comment["author"],
                "created_at": pd.to_datetime(comment["created_at"]),
                "upvote_count": comment["upvote_count"],
                "downvote_count": comment["downvote_count"],
            }
            comments_data.append(comment_data)

    # DataFrame 생성
    posts_df = pd.DataFrame(posts_data)
    comments_df = pd.DataFrame(comments_data)

    return posts_df, comments_df


def save_to_parquet(
    posts_df: pd.DataFrame,
    comments_df: pd.DataFrame,
    posts_path: str = "parsed/post.parquet",
    comments_path: str = "parsed/comment.parquet",
):
    posts_df.to_parquet(posts_path, index=False)
    comments_df.to_parquet(comments_path, index=False)


def main():
    # extracted 디렉토리의 경로 설정
    extracted_path = "./extracted"

    # JSON 파일들 읽기
    extracted_data = read_json_files(extracted_path)

    if not extracted_data:
        print("No data found in the extracted directory")
        return

    # 데이터 처리
    posts_df, comments_df = process_data(extracted_data)

    # 결과 출력
    print(f"Processing completed:")
    print(f"- Number of posts: {len(posts_df)}")
    print(f"- Number of comments: {len(comments_df)}")

    # Parquet 파일로 저장
    save_to_parquet(posts_df, comments_df)
    print("Files saved successfully:")
    print("- post.parquet")
    print("- comment.parquet")


if __name__ == "__main__":
    main()
