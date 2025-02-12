# AWS Lambda Python 3.11 이미지 사용
FROM public.ecr.aws/lambda/python:3.11
# 작업 디렉토리 설정
WORKDIR /var/task

# 소스 코드 복사
COPY main.py .

# Python 패키지 설치
RUN pip install --no-cache-dir beautifulsoup4

# AWS Lambda Python 핸들러 지정 (파일 이름.함수 이름)
CMD ["main.lambda_handler"]