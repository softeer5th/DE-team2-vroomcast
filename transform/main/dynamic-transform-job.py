from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import argparse
import os

def diff_dynamic_post(prev_dynamic_post_df, post_dynamic_post_df):
    # Outer join on 'id'
    joined_df = prev_dynamic_post_df.alias("left").join(
        post_dynamic_post_df.alias("right"), on="id", how="outer"
    )

    # 공통 컬럼 리스트 (id 제외)
    common_columns = list(set(prev_dynamic_post_df.columns) & set(post_dynamic_post_df.columns))
    common_columns.remove("id")  # id는 제외
    common_columns.remove("extracted_at")

    # 차이 계산 (left 값이 없으면 right 값 그대로 사용)
    diff_columns = [
        when(col(f"left.{column}").isNull(), col(f"right.{column}"))
         .otherwise(col(f"right.{column}") - col(f"left.{column}"))
         .alias(f"diff_{column}")
        for column in common_columns
    ]

    # right.id가 None인 경우 삭제 (즉, 이전에는 있었지만 이후에 없어진 경우)
    filtered_df = joined_df.filter(col("right.id").isNotNull())

    # 최종 결과
    result_df = filtered_df.select("id", col("right.extracted_at").alias("extracted_at"), *diff_columns)

    return result_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--prev_dynamic_posts', nargs='+', help='Input parquet file paths')
    parser.add_argument('--post_dynamic_posts', nargs='+', help='Input parquet file paths')
    parser.add_argument('--prev_dynamic_comments', nargs='+', help='Input parquet file paths')
    parser.add_argument('--post_dynamic_comments', nargs='+', help='Input parquet file paths')
    parser.add_argument('--output_dir', help='Output path')
    # spark = SparkSession.builder.appName("DynamicTransformJob").getOrCreate()
    spark = SparkSession.builder \
        .appName("DynamicTransformJob") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
    args = parser.parse_args()
    bucket = args.bucket
    prev_dynamic_posts = args.prev_dynamic_posts
    post_dynamic_posts = args.post_dynamic_posts
    prev_dynamic_comments = args.prev_dynamic_comments
    post_dynamic_comments = args.post_dynamic_comments
    output_dir = args.output_dir
    mode = "s3a://"
    s3_prev_dynamic_posts = [f"{mode}{bucket}/{prev_dynamic_post}" for prev_dynamic_post in prev_dynamic_posts]
    s3_prev_dynamic_comments = [f"{mode}{bucket}/{prev_dynamic_comment}" for prev_dynamic_comment in prev_dynamic_comments]
    s3_post_dynamic_posts = [f"{mode}{bucket}/{post_dynamic_post}" for post_dynamic_post in post_dynamic_posts]
    s3_post_dynamic_comments = [f"{mode}{bucket}/{post_dynamic_comment}" for post_dynamic_comment in post_dynamic_comments]
    prev_dynamic_post_df = spark.read.parquet(*s3_prev_dynamic_posts)
    post_dynamic_post_df = spark.read.parquet(*s3_post_dynamic_posts)
    prev_dynamic_comment_df = spark.read.parquet(*s3_prev_dynamic_comments)
    post_dynamic_comment_df = spark.read.parquet(*s3_post_dynamic_comments)
    diff_dynamic_post_df = diff_dynamic_post(prev_dynamic_post_df, post_dynamic_post_df)
    diff_dynamic_comment_df = diff_dynamic_post(prev_dynamic_comment_df, post_dynamic_comment_df)
    diff_dynamic_post_df.write.mode("overwrite").parquet(f"s3a://{bucket}/{output_dir}/diff_dynamic_post")
    diff_dynamic_comment_df.write.mode("overwrite").parquet(f"s3a://{bucket}/{output_dir}/diff_dynamic_comment")
