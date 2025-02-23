from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import logging
import argparse
import os

def vector_dynamic_post(before_dynamic_post_df, after_dynamic_post_df):
    if before_dynamic_post_df is None:
        renamed_columns = [
            col(column).alias(f"v_{column}") if column not in {"id", "extracted_at"} else col(column).alias(column)
            for column in after_dynamic_post_df.columns
        ]
        return after_dynamic_post_df.select(*renamed_columns)
    
    # Outer join on 'id'
    joined_df = before_dynamic_post_df.alias("left").join(
        after_dynamic_post_df.alias("right"), on="id", how="outer"
    )

    # 공통 컬럼 리스트 (id 제외)
    common_columns = list(set(before_dynamic_post_df.columns) & set(after_dynamic_post_df.columns))
    common_columns.remove("id")  # id는 제외
    common_columns.remove("extracted_at")

    # 차이 계산 (left 값이 없으면 right 값 그대로 사용)
    vector_columns = [
        when(col(f"left.{column}").isNull(), col(f"right.{column}"))
         .otherwise(col(f"right.{column}") - col(f"left.{column}"))
         .alias(f"v_{column}")
        for column in common_columns
    ]

    # right.id가 None인 경우 삭제 (즉, 이전에는 있었지만 이후에 없어진 경우)
    filtered_df = joined_df.filter(col("right.id").isNotNull())

    # 최종 결과
    result_df = filtered_df.select("id", col("right.extracted_at").alias("extracted_at"), *vector_columns)

    return result_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--before_dynamic_posts', nargs='+', help='Input parquet file paths')
    parser.add_argument('--after_dynamic_posts', nargs='+', help='Input parquet file paths')
    parser.add_argument('--before_dynamic_comments', nargs='+', help='Input parquet file paths')
    parser.add_argument('--after_dynamic_comments', nargs='+', help='Input parquet file paths')
    spark = SparkSession.builder.appName("vroomcast-velocity-job").getOrCreate()
    # spark = SparkSession.builder \
    #     .appName("DynamicTransformJob") \
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #     .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    #     .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    #     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    #     .getOrCreate()
    args = parser.parse_args()
    bucket = args.bucket
    before_dynamic_posts = args.before_dynamic_posts
    after_dynamic_posts = args.after_dynamic_posts
    before_dynamic_comments = args.before_dynamic_comments
    after_dynamic_comments = args.after_dynamic_comments
    s3_before_dynamic_posts = [f"s3://{bucket}/{before_dynamic_post}" for before_dynamic_post in before_dynamic_posts]
    s3_before_dynamic_comments = [f"s3://{bucket}/{before_dynamic_comment}" for before_dynamic_comment in before_dynamic_comments]
    s3_after_dynamic_posts = [f"s3://{bucket}/{after_dynamic_post}" for after_dynamic_post in after_dynamic_posts]
    s3_after_dynamic_comments = [f"s3://{bucket}/{after_dynamic_comment}" for after_dynamic_comment in after_dynamic_comments]
    for s3_before_dynamic_post, s3_after_dynamic_post in zip(s3_before_dynamic_posts, s3_after_dynamic_posts):
        before_dynamic_post_df = None
        try:
            before_dynamic_post_df = spark.read.parquet(s3_before_dynamic_post)
        except AnalysisException:
            logging.info(f"{s3_before_dynamic_post} dosen't exist. Skip this file.")
            before_dynamic_post_df = None
        try:
            after_dynamic_post_df = spark.read.parquet(s3_after_dynamic_post)
            vector_dynamic_post_df = vector_dynamic_post(before_dynamic_post_df, after_dynamic_post_df)
            vector_dynamic_post_df.write.mode("overwrite").parquet(f"{s3_after_dynamic_post.rsplit('/', 1)[0]}/vector_dynamic_post")
        except AnalysisException:
            continue
    for s3_before_dynamic_comment, s3_after_dynamic_comment in zip(s3_before_dynamic_comments, s3_after_dynamic_comments):
        before_dynamic_comment_df = None
        try:
            before_dynamic_comment_df = spark.read.parquet(s3_before_dynamic_comment)
        except AnalysisException:
            logging.info(f"{s3_before_dynamic_comment} dosen't exist. Skip this file.")
            before_dynamic_comment_df = None
        try:
            after_dynamic_comment_df = spark.read.parquet(s3_after_dynamic_comment)
            vector_dynamic_comment_df = vector_dynamic_post(before_dynamic_comment_df, after_dynamic_comment_df)
            vector_dynamic_comment_df.write.mode("overwrite").parquet(f"{s3_after_dynamic_comment.rsplit('/', 1)[0]}/vector_dynamic_comment")
        except AnalysisException:
            continue