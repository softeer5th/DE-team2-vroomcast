#!/bin/bash
set -e  # 에러 발생 시 즉시 종료
sudo yum update -y
echo "Ensuring pip3 is up-to-date..."
python3 -m ensurepip
python3 -m pip install --upgrade pip

sudo python3 -m pip install pyarrow

sudo python3 -m pip install kiwipiepy

sudo python3 -m pip install pandas