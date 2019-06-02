FROM python:3.6.8-slim-stretch

LABEL Name=pykamino Version=0.9.0

WORKDIR /tmp
COPY . .
RUN pip install .
