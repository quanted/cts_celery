# Builds an image for CTS calculator celery worker

ARG version=dev

FROM python:3.10.12-slim

ENV APP_USER=www-data

RUN apt-get update && \
	apt-get install -y build-essential

COPY . /src/

WORKDIR /src

RUN pip install -r requirements.txt

ENV PYTHONPATH /src:$PYTHONPATH
ENV PATH /src:$PATH

# Removes any trace of pip to resolve an open CVE.
# TODO: Generalize this using "find" (see hms).
RUN rm -rf \
    /root/.cache/pip \
    /usr/local/bin/pip \
    /usr/local/bin/pip3.10 \
    /usr/local/bin/pip3 \
    /usr/local/lib/python3.10/site-packages/pip \
    /usr/local/lib/python3.10/site-packages/pip-23.0.1.dist-info

RUN chown -R $APP_USER:$APP_USER /src

USER $APP_USER
