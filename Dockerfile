FROM python:3.6.8-slim-stretch

LABEL Name=pykamino Version=0.9.0

RUN apt-get update && apt-get install libpq-dev gcc -y
WORKDIR /app
COPY . .
RUN pip install .[postgresql]
