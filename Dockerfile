# Builds an image for CTS calculator celery worker

ARG version=dev

FROM python:3.10-alpine

ENV APP_USER=www-data

COPY . /src/

WORKDIR /src

RUN pip install -r requirements.txt

ENV PYTHONPATH /src:$PYTHONPATH
ENV PATH /src:$PATH

RUN rm -rf \
    /root/.cache/pip \
    /usr/local/bin/pip \
    /usr/local/lib/python3.10/site-packages/pip

RUN apk update && \
	apk upgrade

RUN adduser -S $APP_USER -G $APP_USER
RUN chown -R $APP_USER:$APP_USER /src

USER $APP_USER
