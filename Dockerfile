# Builds an image for CTS calculator celery worker

FROM python:3.13.13-slim

ENV APP_USER=www-data
ENV CONDA_ENV="pyenv"

USER root

RUN apt-get update && \
    apt-get install -y \
        pkg-config \
        libcairo2-dev \
        # python3-dev \
        gcc

COPY . /src/
WORKDIR /src


RUN chmod 755 start-worker.sh start-manager.sh

RUN pip install -r requirements.txt

# Security Issues Mitigations
# ------------------------- #
RUN find /opt/conda/pkgs/future* -name "*.pem" -delete || true
RUN find /opt/conda/lib/python3.10/site-packages/future -name "*.pem" -delete || true
# RUN find /opt/conda/envs/pyenv -name "*.pem" -delete || true
RUN find /opt/conda -name "*test.key" -delete || true
RUN find /opt/conda/ -name 'test.key' -delete || true
RUN find /opt/conda/ -name 'localhost.key' -delete || true
RUN find /opt/conda/ -name 'server.pem' -delete || true
RUN find /opt/conda/ -name 'client.pem' -delete || true
RUN find /opt/conda/ -name 'password_protected.pem' -delete || true
# ------------------------- #


RUN chown -R $APP_USER:$APP_USER /src

ENV DJANGO_SETTINGS_MODULE "settings"
EXPOSE 8080

ENV PYTHONPATH /src:$PYTHONPATH
ENV PATH /src:$PATH

USER $APP_USER
