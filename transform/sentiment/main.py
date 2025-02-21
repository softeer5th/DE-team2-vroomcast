from pydantic import BaseModel, Field
from openai import OpenAI
import os
import openai
import time
import logging
import boto3
import io
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
BATCH_SIZE = 20

SCHEMA = pa.schema([
    ('id', pa.string()),              # VARCHAR(255)
    ('source_id', pa.string()),       # VARCHAR(255)
    ('from_post', pa.bool_()),        # BOOLEAN
    ('sentence', pa.string()),        # VARCHAR(2048)
    ('created_at', pa.timestamp('ns')),  # TIMESTAMP
    ('sentiment', pa.int32())         # INT
])

# Pydantic을 활용한 감성 분석 결과 모델 정의
class SentimentAnalysis(BaseModel):
    sentiments: list[int] = Field(
        ..., description="각 문장의 감성 점수 배열 (1: 긍정, 0: 중립, -1: 부정)"
    )

def request_openai_api(full_prompt, len_batch_sentences, client, max_retries=10):
    """
    Sends a prompt to the OpenAI API to classify the sentiment of Korean sentences.
    
    Constructs and sends a chat completion request to the OpenAI API using a system prompt that instructs the model to classify each sentence as positive (1), negative (-1), or neutral (0). If the number of sentiment scores returned does not match the expected count or if an error occurs, a default list of zeros is returned. The function retries on rate limit errors using an exponential backoff strategy.
    
    Args:
        full_prompt (str): The user prompt containing sentences to analyze.
        len_batch_sentences (int): Expected number of sentiment scores (i.e., number of sentences).
        client: An OpenAI API client instance that supports the beta chat completions interface.
        max_retries (int, optional): Maximum number of retry attempts on encountering rate limits.
            Defaults to 10.
    
    Returns:
        list[int]: A list of sentiment scores corresponding to the input sentences. Returns a
        default list of zeros if the response is invalid or an error occurs.
    
    Example:
        >>> sentiments = request_openai_api(prompt, 5, openai_client)
        >>> print(sentiments)
        [1, 0, -1, 1, 0]
        
    Note:
        API rate limit errors trigger retries with an exponential backoff delay. Other exceptions
        are logged and result in the default sentiment list.
    """
    for attempt in range(max_retries):
        print(f"Attempt {attempt + 1}/{max_retries}...")
        try:
            response = client.beta.chat.completions.parse(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system",
                     "content": "당신의 작업은 주어진 한국어 문장의 감성을 긍정(1), 부정(-1), 중립(0)으로 분류하고 배열 형식으로 숫자 값만 반환하는 것입니다."},
                    {"role": "user", "content": full_prompt},
                ],
                response_format=SentimentAnalysis,
            )
            sentiments = response.choices[0].message.parsed.sentiments
            # 응답 개수가 입력 개수와 다를 경우 기본값 반환
            if len(sentiments) != len_batch_sentences:
                logger.warning(f"[Error Open AI] 응답 개수 불일치: 입력({len_batch_sentences}) vs 응답({len(sentiments)})")
                return [0] * len_batch_sentences

            logger.info(f"[Rated] Sentiments: {sentiments}, attempt: {attempt + 1}/{max_retries}")
            return sentiments

        except openai.RateLimitError as e:
            wait_time = 1.5 ** attempt
            logger.error(f"[Rate Limit 초과] {e}. {wait_time}초 후 재시도... ({attempt + 1}/{max_retries})")
            time.sleep(wait_time)

        except Exception as e:
            logger.error(f"[Error Open AI] {e}")
            return [0] * len_batch_sentences
    logger.error("[Error Open AI] 최대 재시도 횟수를 초과했습니다.")
    return [0] * len_batch_sentences

def analyze_sentiments(client, sentences):
    """
    Performs batch-based sentiment analysis on a list of sentences.
    
    Divides the provided sentences into batches defined by the global constant BATCH_SIZE and generates a prompt for each batch to classify the sentiment of each sentence as positive (1), negative (-1), or neutral (0). The prompt instructs the OpenAI API to return an array of sentiment scores matching the number of sentences in the batch. If the input list is empty or None, returns an empty list.
    
    Args:
        client: OpenAI API client used to perform the sentiment analysis request.
        sentences (List[str]): List of sentences to analyze.
    
    Returns:
        List[int]: A list of sentiment scores corresponding to each input sentence.
    """
    total_sentiments = []
    if sentences is None or len(sentences) == 0:
        return []

    for i in range(0, len(sentences), BATCH_SIZE):
        print(f"Processing {i} to {i+BATCH_SIZE}...")
        batch_sentences = sentences[i:i+BATCH_SIZE]
        # Batch 요청을 위한 프롬프트 생성
        prompt = "\n".join([f'{idx + 1}번째 문장 >> "{sent}"' for idx, sent in enumerate(batch_sentences)])

        full_prompt = f"""
        다음은 한국어 문장 목록입니다. 각 문장의 감성을 긍정(1), 부정(-1), 중립(0) 중 하나로 분류하세요.
        응답은 배열 형식으로 숫자 값만 반환합니다. 
        !!! 응답의 형식은 반드시 입력된 문장의 개수({len(batch_sentences)})와 동일한 길이의 배열이어야 합니다. !!!
        각 번호에 해당하는 감정은 다음과 같은 양식으로만 작성해야 합니다:
        [1, -1, 0, …]
    
        문장 개수: {len(batch_sentences)}
        다음은 문장 목록입니다:
        {prompt}
        """
        total_sentiments.extend(request_openai_api(full_prompt, len(batch_sentences), client))

    return total_sentiments

def process_parquet(bucket_name, input_key, output_key):
    """
    S3에서 Parquet 파일을 읽어 감정 분석 결과를 저장합니다.
    
    Args:
    	bucket_name (str): S3 버킷 이름.
    	input_key (str): 입력 Parquet 파일의 S3 키.
    	output_key (str): 처리된 파일을 저장할 S3 키.
    
    Raises:
    	ValueError: DataFrame에 'sentence' 컬럼이 없을 경우 발생.
    
    Side Effects:
    	입력 Parquet 파일을 S3에서 읽은 후 문장에 대한 감정 분석을 수행하고,
    	새로운 sentiment 컬럼을 추가하여 결과 파일을 S3에 업로드합니다.
    """
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    # S3에서 Parquet 파일 읽기
    obj = s3_client.get_object(Bucket=bucket_name, Key=input_key)
    parquet_data = io.BytesIO(obj['Body'].read())
    df = pd.read_parquet(parquet_data)

    # 'sentence' 컬럼이 존재하는지 확인
    if 'sentence' not in df.columns:
        raise ValueError("'sentence' 컬럼이 존재하지 않습니다.")
    logger.info(f"{input_key} read successfully. {len(df)} rows.")
    print(f"{input_key} read successfully. {len(df)} rows.")
    df["sentiment"] = analyze_sentiments(client, df["sentence"].tolist())
    print(df["sentiment"].value_counts())
    data = {
        "id": df["id"].astype(str),
        "source_id": df["source_id"].astype(str),
        "from_post": df["from_post"].astype(bool),
        "sentence": df["sentence"].astype(str),
        "created_at": pd.to_datetime(df["created_at"]),
        "sentiment": df["sentiment"].astype("int32"),
    }
    new_df = pd.DataFrame(data)
    table = pa.Table.from_pandas(new_df, schema=SCHEMA)  # 적용된 스키마를 사용하여 Table 생성

    # PyArrow를 활용한 Parquet 파일 작성
    output_buffer = io.BytesIO()
    pq.write_table(table, output_buffer)
    output_buffer.seek(0)

    # 결과를 S3에 업로드
    s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=output_buffer)
    logger.info(f"처리 결과를 S3에 저장 완료: s3://{bucket_name}/{output_key}")

def process_all_files(bucket_name, input_prefix, output_prefix):
    """
    S3 버킷 내 특정 프리픽스의 모든 Parquet 파일을 처리합니다.
    
    Args:
        bucket_name (str): S3 버킷의 이름.
        input_prefix (str): 처리할 Parquet 파일들이 위치한 S3 객체 키의 프리픽스.
        output_prefix (str): 처리된 파일을 저장할 S3 객체 키의 프리픽스.
            입력 프리픽스를 해당 출력 프리픽스로 대체하여 파일 이름을 재정의합니다.
    
    파일 목록을 페이지 단위로 조회하며, 각 페이지 내에서 .parquet 확장자로 끝나는 파일에 대해
    `process_parquet` 함수를 호출합니다. 파일 처리 중 오류가 발생하면 에러 로그를 기록하며,
    지정된 프리픽스에 파일이 없으면 경고 로그를 남깁니다.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=input_prefix)

    for page in page_iterator:
        if "Contents" in page:  # 파일이 존재할 경우
            for obj in page["Contents"]:
                input_key = obj["Key"]

                if input_key.endswith(".parquet"):  # Parquet 파일만 처리
                    # Output 파일 이름 정의 (input_prefix를 output_prefix로 대체)
                    output_key = input_key.replace(input_prefix, output_prefix, 1)
                    try:
                        print(f"Processing {input_key}...")
                        process_parquet(bucket_name, input_key, output_key)
                        print(f"Processed {input_key} successfully.")
                    except Exception as e:
                        logger.error(f"파일 처리 실패: {input_key}, 에러: {e}")
        else:
            logger.warning(f"{input_prefix} 하위에 파일이 없습니다.")

def lambda_handler(event, context):
    """
    Entry point for AWS Lambda handling sentiment analysis on S3 Parquet files.
    
    Extracts S3 bucket, input directory, and output directory information from the event,
    processes the Parquet files for sentiment analysis, and stores the results to the 
    specified S3 output directory. Returns a response with an HTTP status code and a message
    indicating the outcome of the operation.
    
    Args:
        event (dict): AWS Lambda event containing the following keys:
            - 'bucket_name' (str): Name of the S3 bucket to process.
            - 'input_dir' (str): Directory in the S3 bucket containing the input Parquet files.
            - 'output_dir' (str): Directory in the S3 bucket where the processed files will be saved.
        context (Any): AWS Lambda context object providing runtime information (unused in this function).
    
    Returns:
        dict: A dictionary with the following keys:
            - 'statusCode' (int): 200 if processing is successful; 500 if an error occurs.
            - 'body' (str): A message indicating either the success result with S3 path or error details.
    
    Example:
        >>> event = {
        ...     "bucket_name": "example-bucket",
        ...     "input_dir": "input/parquet",
        ...     "output_dir": "output/sentiment"
        ... }
        >>> context = {}  # AWS Lambda context placeholder
        >>> lambda_handler(event, context)
        {'statusCode': 200, 'body': 'Sentiment analysis completed and saved to s3://example-bucket/output/sentiment/'}
    """
    # 이벤트에서 S3 Bucket과 Key 파라미터 추출
    bucket_name = event['bucket_name']
    input_dir = event['input_dir']
    output_dir = event['output_dir']

    try:
        # S3에서 Parquet 데이터를 처리하고 저장
        logger.info(f"S3 처리 시작: bucket={bucket_name}, input_dir={input_dir}, output_dir={output_dir}")
        process_all_files(bucket_name, input_dir, output_dir)
        return {
            "statusCode": 200,
            "body": f"Sentiment analysis completed and saved to s3://{bucket_name}/{output_dir}/"
        }
    except Exception as e:
        logger.error(f"예외 발생: {e}")
        return {
            "statusCode": 500,
            "body": f"Error occurred: {e}"
        }