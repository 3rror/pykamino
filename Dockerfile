FROM python:3.6-slim

LABEL name=pykamino version=0.9.0

RUN apt-get update \
  && apt-get install --no-install-recommends -y libpq-dev gcc libc-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt /app
RUN pip install -r requirements.txt
COPY . /app
RUN pip install .[postgresql]
