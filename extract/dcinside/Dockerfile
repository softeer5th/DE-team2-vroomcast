# AWS Lambda Python 3.11 이미지 사용
FROM public.ecr.aws/lambda/python:3.11

# 필수 패키지 설치
RUN yum install -y \
    unzip \
    libnss3 \
    alsa-lib \
    atk \
    cups-libs \
    gtk3 \
    libXcomposite \
    libXcursor \
    libXdamage \
    libXext \
    libXi \
    libXrandr \
    libXScrnSaver \
    libXtst \
    pango \
    freetype \
    nss \
    fontconfig \
    wget \
    glib2 \
    libX11 \
    libstdc++.so.6




# 의존성 파일 복사 (requirements.txt가 있다면 사용)
COPY requirements.txt .



# 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# 크롤링에 필요한 크롬 헤드리스 및 드라이버 추가
# ChromeDriver 복사
COPY chromedriver-mac-arm64/chromedriver /opt/chromedriver
# chrome-headless-shell 폴더 복사
COPY chrome-headless-shell-mac-arm64 /opt/chrome/

RUN chmod +x /opt/chrome && \
chmod +x /opt/chromedriver

# 작업 디렉토리 설정
# WORKDIR /var/task
    
# 소스 코드 추가
COPY main.py /var/task


# AWS Lambda Python 핸들러 지정 (파일 이름.함수 이름)
CMD ["main.lambda_handler"]