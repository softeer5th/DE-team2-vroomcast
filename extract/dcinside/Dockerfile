FROM amazon/aws-lambda-python:3.12
# Install chrome dependencies
RUN dnf install -y atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel nss mesa-libgbm jq unzip dos2unix

# Copy and run the chrome installer script
COPY ./chrome-installer.sh ./chrome-installer.sh
RUN chmod +x ./chrome-installer.sh
RUN ./chrome-installer.sh
RUN rm ./chrome-installer.sh

# Install selenium
RUN pip install selenium boto3 pandas bs4 python-dotenv

# Copy the main application code
COPY driver_test.py ./
COPY main.py ./
# # AWS Lambda Python 핸들러 지정 (파일 이름.함수 이름)
CMD ["main.lambda_handler"]
# CMD ["driver_test.lambda_handler"]