from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import argparse

def diff_dynamic_post(prev_dynamic_post_df, post_dynamic_post_df):
    # Outer join on 'id'
    joined_df = prev_dynamic_post_df.alias("left").join(
        post_dynamic_post_df.alias("right"), on="id", how="outer"
    )

    # 공통 컬럼 리스트 (id 제외)
    common_columns = list(set(prev_dynamic_post_df.columns) & set(post_dynamic_post_df.columns))
    common_columns.remove("id")  # id는 제외

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
    result_df = filtered_df.select("id", *diff_columns)

    return result_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--input_prev_dynamic_post', nargs='+', help='Input parquet file paths')
    parser.add_argument('--input_post_dynamic_post', nargs='+', help='Input parquet file paths')
    parser.add_argument('--input_prev_dynamic_comment', nargs='+', help='Input parquet file paths')
    parser.add_argument('--input_post_dynamic_comment', nargs='+', help='Input parquet file paths')
    parser.add_argument('--output_dir', help='Output path')
    spark = SparkSession.builder.appName("DynamicTransformJob").getOrCreate()
    args = parser.parse_args()
    bucket = args.bucket
    input_prev_dynamic_post = args.input_prev_dynamic_post
    input_post_dynamic_post = args.input_post_dynamic_post
    input_prev_dynamic_comment = args.input_prev_dynamic_comment
    input_post_dynamic_comment = args.input_post_dynamic_comment
    output_dir = args.output_dir
    prev_dynamic_post_df = spark.read.parquet(input_prev_dynamic_post)
    prev_dynamic_comment_df = spark.read.parquet(input_prev_dynamic_comment)
    post_dynamic_post_df = spark.read.parquet(input_post_dynamic_post)
    post_dynamic_comment_df = spark.read.parquet(input_post_dynamic_comment)
    diff_dynamic_post_df = diff_dynamic_post(prev_dynamic_post_df, post_dynamic_post_df)
    diff_dynamic_comment_df = diff_dynamic_post(prev_dynamic_comment_df, post_dynamic_comment_df)
    diff_dynamic_post_df.write.parquet(f"s3://{bucket}/{output_dir}/diff_dynamic_post")
    diff_dynamic_comment_df.write.parquet(f"s3://{bucket}/{output_dir}/diff_dynamic_comment")
