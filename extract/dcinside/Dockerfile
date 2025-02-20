FROM amazon/aws-lambda-python:3.12
# Install chrome dependencies
RUN dnf install -y atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel nss mesa-libgbm jq unzip

# Copy and run the chrome installer script
COPY ./chrome-installer.sh ./chrome-installer.sh
RUN chmod +x ./chrome-installer.sh
RUN ./chrome-installer.sh
RUN rm ./chrome-installer.sh

# Install selenium
RUN pip install selenium boto3 pandas bs4 python-dotenv

# Copy the main application code
COPY driver_test.py ./
# COPY crawler/ ./crawler/

# Command to run the Lambda function
CMD [ "main.lambda_handler" ]
#=============================================================================
# # AWS Lambda Python 3.11 이미지 사용
# FROM amazon/aws-lambda-python:3.11

# # 필수 패키지 설치
# RUN yum install -y atk cups-libs gtk3 libXcomposite alsa-lib \
#     libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
#     libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
#     xorg-x11-xauth dbus-glib dbus-glib-devel nss mesa-libgbm jq unzip

# # RUN yum installer -y \
# #     libnss3 \
# #     alsa-lib \
# #     atk \
# #     cups-libs \
# #     gtk3 \
# #     freetype \
# #     nss \
# #     fontconfig \
# #     wget \
# #     glib2 \
# #     libX11 \
# #     libstdc++.so.6


# # Copy and run the chrome installerer script
# COPY ./chrome-installer.sh ./chrome-installer.sh
# RUN chmod +x ./chrome-installer.sh
# RUN ./chrome-installer.sh
# RUN rm ./chrome-installer.sh

# # 의존성 파일 복사 (requirements.txt가 있다면 사용)
# COPY requirements.txt .



# # 패키지 설치
# RUN pip install --no-cache-dir -r requirements.txt

# # 크롤링에 필요한 크롬 헤드리스 및 드라이버 추가
# # ChromeDriver 복사
# # COPY chromedriver-mac-arm64/chromedriver /opt/chromedriver/
# # COPY chrome-headless-shell-mac-arm64 /opt/chrome/
# # COPY chromedriver-linux64/chromedriver /opt/chromedriver
# # COPY chrome-headless-shell-linux64 /opt/chrome/
# # chrome-headless-shell 폴더 복사

# RUN chmod +x /opt/chrome && \
#     chmod +x /opt/chrome-driver

# # 작업 디렉토리 설정
# # WORKDIR /var/task
    
# # 소스 코드 추가
# COPY main.py /var/task
# COPY refactor3.py /var/task
# COPY driver_test.py /var/task


# # AWS Lambda Python 핸들러 지정 (파일 이름.함수 이름)
# CMD ["main.lambda_handler"]