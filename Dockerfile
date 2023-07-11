# Builds an image for CTS calculator celery worker

# FROM python:3.10-alpine
FROM python:3.10.12-slim

ENV APP_USER=www-data

COPY . /src/

WORKDIR /src


RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y build-essential

RUN pip install -r requirements.txt
RUN pip install --upgrade pip

RUN chown -R $APP_USER:$APP_USER /src

ENV PYTHONPATH /src:$PYTHONPATH
ENV PATH /src:$PATH

USER $APP_USER
