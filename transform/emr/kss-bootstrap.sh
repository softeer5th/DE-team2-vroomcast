#!/bin/bash
set -e  # 에러 발생 시 즉시 종료
sudo yum update -y

sudo yum install -y python3-devel

echo "Ensuring pip3 is up-to-date..."
python3 -m ensurepip
python3 -m pip install --upgrade pip

echo "Installing latest wheel package..."
sudo python3 -m pip install --upgrade wheel

echo "Installing KSS (Korean Sentence Splitter)..."
sudo python3 -m pip install kss

sudo python3 -m pip install pandas