FROM python:3.6

LABEL Name=pykamino Version=0.0.1

RUN mkdir /app
WORKDIR /app
ADD . /app
RUN python3 -m pip install .
