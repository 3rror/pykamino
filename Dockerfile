FROM python:3.6.8-slim-stretch

LABEL Name=pykamino Version=0.9.0

RUN apt-get update && apt-get install -y libpq-dev gcc
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
COPY . /app
RUN pip install .[postgresql]
