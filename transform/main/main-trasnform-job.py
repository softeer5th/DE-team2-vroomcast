from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from openai import OpenAI
import kss
import pandas as pd
import os
import json
import argparse
import logging

logger = logging.getLogger(__name__)

# Pandas UDF의 반환 타입을 StructType으로 지정
keyword_dict = {
    "design": [
        "디자인", "디쟌", "외관", "외형", "내장", "내장재", "내외관", "생김새", "신형", "구형",
        "풀체", "풀체인지", "페리", "페이스리프트", "변경", "형상", "실루엣", "룩", "스타일", "스타일링",
        "패밀리룩", "간지", "깔쌈", "스포티", "바디킷", "스포일러", "사이드스커트", "리어스커트", "루프", "루프스포일러",
        "루프랙", "도어", "손잡이", "시트", "휠", "휠베이스", "알휠", "헤드라이트", "라이트", "LED",
        "HID", "DRL", "데이라이트", "테일램프", "후미등", "백라", "안개등", "그릴", "프론트범퍼", "범퍼",
        "에어로", "공기역학", "에어댐", "볼륨감", "고급감", "포인트컬러", "루프컬러", "실내", "대시보드", "센터페시아",
        "스티어링휠", "핸들디자인", "계기판", "앰비언트라이트", "우드트림", "메탈트림", "가죽시트", "패브릭시트", "나파가죽", "시트패턴",
        "착좌감", "조수석", "2열", "3열", "트렁크", "공간감", "개방감", "파노라마선루프", "썬루프", "무드등",
        "재질", "마감", "엠블럼", "투톤", "바디컬러", "블랙베젤", "사이드미러", "리어스포일러", "와이드", "슬림",
        "루프라인", "실물", "프로포션", "유니크", "청담감성", "현대스러움", "모던", "미래지향", "형태", "인테리어",
        "익스테리어", "아이덴티티", "N라인", "턴시그널", "에어벤트", "사이드가니쉬", "테일", "보닛", "허니콤그릴", "스플리터"
    ],
    "performance": [
        "성능", "출력", "토크", "마력", "속도", "가속", "제동", "브레이크", "승차감", "핸들링",
        "코너링", "서스펜션", "쇼바", "하체", "엔진", "미션", "변속", "수동", "자동", "CVT",
        "DCT", "변속타이밍", "수동모드", "패들시프트", "파워트레인", "터보", "슈퍼차저", "자연흡기", "하이브리드", "PHEV",
        "EV", "연비", "연료효율", "주행거리", "배터리", "배기음", "배기", "배기가스", "배출가스", "RPM",
        "엔진음", "소음", "진동", "트랙션", "그립", "사륜", "후륜", "전륜", "AWD", "4WD",
        "LSD", "자율주행", "ADAS", "차선유지", "어댑티브크루즈", "HUD", "TCS", "ESC", "ABS", "언더스티어",
        "오버스티어", "제어", "반응속도", "가속페달", "브레이크페달", "오토홀드", "EPB", "재시동", "에코모드", "스포츠모드",
        "N모드", "드라이브모드", "컴포트모드", "파워", "토컨", "발진", "제동력", "오버부스트", "부스트", "맵핑",
        "튜닝", "흡기", "인젝터", "인터쿨러", "하이브리드시스템", "모터출력", "등판능력", "언덕길", "고속주행", "시내주행",
        "장거리", "차체강성", "충격흡수", "전기모터", "드리프트", "드라이빙", "발열", "냉각", "수온", "파워",
        "승차", "브렘보", "제로백", "크루즈", "안전", "내구성", "파워", "네비", "가속도", "반자율"
    ],
    "price": [
        "가격", "비쌈", "고가", "저가", "저렴", "가성비", "가심비", "창렬", "혜자", "트림",
        "깡통", "풀옵", "돈값", "돈낭비", "유지비", "정비비", "수리비", "부품비", "보험료", "세금",
        "취득세", "등록세", "개소세", "부가세", "고정비", "변동비", "할부", "리스", "렌트", "중고",
        "중고시세", "중고감가", "잔존가치", "보증", "보증연장", "보증기간", "할인이벤트", "프로모", "특판", "출고가",
        "실구매가", "계약금", "잔금", "월납입금", "잔가", "영맨할인", "파이낸스", "이자", "금리", "고정금리",
        "변동금리", "무이자", "포인트", "캐쉬백", "보조금", "전기보조금", "수소차보조금", "폐차보조금", "감가", "리세일",
        "매도", "매입", "딜러", "매물", "시세", "호갱", "호구", "깎기", "가격협상", "신차가",
        "옵션가", "악옵", "옵션질", "유료옵션", "풀패키지", "결제", "카드결제", "현금결제", "견적", "출력영수증",
        "트레이드인", "중고보상", "업그레이드", "다운그레이드", "세이브", "구입비", "초기비용", "유지관리", "그돈씨", "극할인",
        "사전계약", "계약대기", "출고대기", "재고정리", "재고차", "무옵션", "할부금", "인수거부", "잔가보장", "잔가형리스", "이천", "2천", "삼천", "3천", "사천", "4천", "오천", "5천", "육천", "6천", "칠천", "7천", "팔천", "8천", "구천", "9천"
    ],
    "malfunction": [
        "결함", "고장", "하자", "불량", "리콜", "누유", "누수", "잡소리", "이상음", "소음",
        "진동", "떨림", "과열", "시동", "급발", "전기", "전자", "배터리", "경고등", "배선",
        "수리", "오작동", "에러코드", "미션", "엔진", "클러치", "브레이크", "디스크", "오일",
        "냉각수", "흡기", "배기", "촉매", "DPF", "EGR", "ECU", "ICCU", "퓨즈", "AS",
        "홈링", "배기가스", "과도연소", "실린더", "피스톤", "부싱", "로어암", "데프", "조향", "얼라인먼트",
        "유격", "후방카메라", "비상등", "에어컨", "히터", "와이퍼", "뒷문", "도어락", "도장", "코팅",
        "블박", "운전석", "누적결함", "배터리방전", "계기판", "RPM", "밀림", "오일필터", "장착",
        "에어백", "실내등", "공조장치", "진공펌프", "융착", "와이어", "차체", "트렁크", "오버히트", "고장코드",
        "노이즈", "부식", "녹", "무반응", "인식오류", "키인식", "연료계통", "펌프", "필터", "갈림",
        "오일누유", "소프트웨어", "업그레이드오류", "모듈오류", "점화", "체인", "체인늘어짐", "결함판정", "재발", "위험"
    ]
}


def analyze_sentiments(sentences):
    """OpenAI API를 Batch로 호출하여 감성 분석 후 숫자 배열로 반환"""
    if not sentences:
        return []

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Batch 요청을 위한 프롬프트 생성
    prompt = "\n".join([f'{idx + 1}번째 문장 >> "{sent}"' for idx, sent in enumerate(sentences)])

    full_prompt = f"""다음은 한국어 문장 목록입니다. 각 문장의 감성을 긍정(1), 부정(-1), 중립(0) 중 하나로 분류하세요.
    응답은 JSON 배열 형식으로 숫자 값만 반환하세요.

    예시 응답:
    [1, 0, -1]
    
    답변할 문장의 개수 : {len(sentences)}

    문장 목록:
    {prompt}
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system",
                 "content": "당신의 작업은 주어진 커뮤니티에서 나온 자동차 관련 한국어 문장의 감성을 긍정(1), 부정(-1), 중립(0)으로 분류하고 배열 형식으로 숫자 값만 반환하는 것입니다."},
                {"role": "user", "content": full_prompt}
            ],
            temperature=0,  # 정확성을 위해 낮게 설정
            max_tokens=50,  # 적절한 크기 설정
            top_p=1
        )
        # print(f"[OpenAI API] {response.choices[0].message.content}")
        sentiments = json.loads(response.choices[0].message.content)

        # 결과 개수가 입력 개수와 다를 경우 기본값 반환
        if len(sentiments) != len(sentences):
            logger.warning(f"[Error Open AI] {sentiments}, {len(sentiments)}, {prompt}, {len(sentences)}")
            return [0] * len(sentences)

        return sentiments

    except Exception as e:
        logger.error(f"[Error Open AI] {e}")
        return [0] * len(sentences)

@pandas_udf(ArrayType(
    StructType([
        StructField("id", StringType(), False),
        StructField("sentence", StringType(), True),
        StructField("from_post", BooleanType(), True),
        StructField("sentiment", IntegerType(), True)
    ])
))
def get_sentences_and_sentiments(source_id: pd.Series, title: pd.Series, content: pd.Series) -> pd.Series:
    results = []
    for source_id, title, content in zip(source_id, title, content):
        result_per_content = []
        sentences = kss.split_sentences(title) if title else []
        content_sentences = kss.split_sentences(content) if content else []
        sentences.extend(content_sentences)
        sentiments = analyze_sentiments(sentences)
        idx = 0
        for sentence, sentiment in zip(sentences, sentiments):
            result_per_content.append((f"{source_id}_{idx}", sentence, True if title else False, sentiment))
            idx += 1
        results.append(result_per_content)
    return pd.Series(results)


@pandas_udf(ArrayType(
    StructType([
        StructField("category", StringType(), True),
        StructField("keyword", StringType(), True)
    ])
))
def extract_category_keyword(sentences: pd.Series) -> pd.Series:
    """KSS로 문장을 분리하고, 키워드를 찾은 후 튜플을 반환"""
    results = []
    for sentence in sentences:
        result_per_sentence = []
        for category, keywords in keyword_dict.items():
            for keyword in keywords:
                if keyword in sentence:
                    result_per_sentence.append((category, keyword))
        results.append(result_per_sentence)
    return pd.Series(results)

# 링크 필터링, 유저 필터링
def regex_replace_privacy(df):
    df = df.withColumn("content", regexp_replace(col("content"), r'https?://\S+', ''))
    df = df.withColumn("content", regexp_replace(col("content"), r'@[\w\uac00-\ud7af]+', ''))
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--input_post_paths', nargs='+', help='Input parquet file paths')
    parser.add_argument('--input_comment_paths', nargs='+', help='Input parquet file paths')
    parser.add_argument('--output_dir', help='Output path')
    args = parser.parse_args()
    mode = "s3://"
    bucket = args.bucket
    input_post_paths = args.input_post_paths
    input_comment_paths = args.input_comment_paths
    output_dir = args.output_dir
    s3_input_post_paths = [f"{mode}{bucket}/{input_post_path}" for input_post_path in input_post_paths]
    s3_input_comment_paths = [f"{mode}{bucket}/{input_comment_path}" for input_comment_path in input_comment_paths]
    spark = SparkSession.builder.appName("vroomcast-spliter-job").getOrCreate()
    # spark = SparkSession.builder \
    #     .appName("vroomcast-spliter-job") \
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #     .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    #     .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    #     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    #     .getOrCreate()
    post_df = spark.read.parquet(*s3_input_post_paths)
    comment_df = spark.read.parquet(*s3_input_comment_paths)
    logger.info(f"Post and Comment Read Complete.")
    post_df = regex_replace_privacy(post_df)
    comment_df = regex_replace_privacy(comment_df)
    post_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/post_static")
    comment_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/comment_static")
    logger.info(f"Post and Comment Static Write Complete. {mode}{bucket}/{output_dir}/post_static, {mode}{bucket}/{output_dir}/comment_static")
    pre_sentence_df = post_df.selectExpr("id AS source_id", "title", "content", "created_at")
    pre_sentence_df = pre_sentence_df.union(
        comment_df.selectExpr("id AS source_id","CAST(NULL AS STRING) AS title", "content", "created_at")
    )
    sentence_df = pre_sentence_df.withColumn(
        "sentences",
        explode(
            get_sentences_and_sentiments(
                col("source_id"), col("title"), col("content")
            )
        )
    ).selectExpr("sentences.id AS id", "source_id", "sentences.from_post AS from_post", "sentences.sentence AS sentence", "created_at","sentences.sentiment AS sentiment")
    sentence_df.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/sentence")
    logger.info(f"Sentence Table Write Complete. {mode}{bucket}/{output_dir}/sentence")
    sentence_df.cache()
    sentence_processed = sentence_df.withColumn(
        "keyword_category",
        explode(
            extract_category_keyword(col("sentence"))
        )
    ).selectExpr("id AS sentence_id", "keyword_category.*")
    sentence_processed.write.mode("overwrite").parquet(f"{mode}{bucket}/{output_dir}/sentence_processed")
    logger.info(f"Sentence Processed Table Write Complete. {mode}{bucket}/{output_dir}/sentence_processed")
    spark.stop()

