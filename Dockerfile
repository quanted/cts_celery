# Builds an image for CTS calculator celery worker

ARG version=dev

FROM python:3.9-alpine

COPY . /src/

WORKDIR /src

RUN pip install -r requirements.txt
