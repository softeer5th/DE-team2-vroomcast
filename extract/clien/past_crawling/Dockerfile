# AWS Lambda Python 3.11 이미지 사용
FROM public.ecr.aws/lambda/python:3.11

# 작업 디렉토리 설정
WORKDIR /var/task

# 소스 코드 추가
COPY extract_old_data.py .
COPY parse_html.py .

# 의존성 파일 복사 (requirements.txt가 있다면 사용)
COPY requirements.txt .

# 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# AWS Lambda Python 핸들러 지정 (파일 이름.함수 이름)
CMD ["extract_old_data.lambda_handler"]