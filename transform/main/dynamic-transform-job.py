from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import argparse
import os

def vector_dynamic_post(before_dynamic_post_df, after_dynamic_post_df):
    # Outer join on 'id'
    """
    Compute differences of shared columns between pre- and post-event DataFrames.
    
    Performs an outer join on the 'id' column and identifies common columns (excluding
    'id' and 'extracted_at'). For each common column, creates a new column prefixed with
    'v_' that subtracts the pre-event value (left DataFrame) from the post-event value (right
    DataFrame), using the post-event value directly if the pre-event value is null. Rows where
    the post-event 'id' is null are filtered out.
    
    Args:
        before_dynamic_post_df (DataFrame): Pre-event DataFrame of dynamic posts.
        after_dynamic_post_df (DataFrame): Post-event DataFrame of dynamic posts.
    
    Returns:
        DataFrame: A DataFrame containing 'id', 'extracted_at' from the post-event DataFrame,
        and new columns 'v_<column>' for each shared column representing the computed differences.
    """
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
    parser.add_argument('--output_dir', help='Output path')
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
    output_dir = args.output_dir
    s3_before_dynamic_posts = [f"s3://{bucket}/{before_dynamic_post}" for before_dynamic_post in before_dynamic_posts]
    s3_before_dynamic_comments = [f"s3://{bucket}/{before_dynamic_comment}" for before_dynamic_comment in before_dynamic_comments]
    s3_after_dynamic_posts = [f"s3://{bucket}/{after_dynamic_post}" for after_dynamic_post in after_dynamic_posts]
    s3_after_dynamic_comments = [f"s3://{bucket}/{after_dynamic_comment}" for after_dynamic_comment in after_dynamic_comments]
    before_dynamic_post_df = spark.read.parquet(*s3_before_dynamic_posts)
    after_dynamic_post_df = spark.read.parquet(*s3_after_dynamic_posts)
    before_dynamic_comment_df = spark.read.parquet(*s3_before_dynamic_comments)
    after_dynamic_comment_df = spark.read.parquet(*s3_after_dynamic_comments)
    vector_dynamic_post_df = vector_dynamic_post(before_dynamic_post_df, after_dynamic_post_df)
    vector_dynamic_comment_df = vector_dynamic_post(before_dynamic_comment_df, after_dynamic_comment_df)
    vector_dynamic_post_df.write.mode("overwrite").parquet(f"s3://{bucket}/{output_dir}/vector_dynamic_post")
    vector_dynamic_comment_df.write.mode("overwrite").parquet(f"s3://{bucket}/{output_dir}/vector_dynamic_comment")
