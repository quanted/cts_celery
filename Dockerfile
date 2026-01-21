# Builds an image for CTS calculator celery worker

FROM mambaorg/micromamba:2.5-alpine3.22

ENV APP_USER=www-data
ENV CONDA_ENV="pyenv"

USER root

RUN adduser -S $APP_USER -G $APP_USER

RUN apk add --update --no-cache \
    build-base \
    jpeg-dev \
    zlib-dev \
    libjpeg \
    gettext \
    linux-headers \
    && rm -rf /var/cache/apk/*

COPY . /src/
WORKDIR /src


RUN chmod 755 start-worker.sh start-manager.sh

RUN micromamba create -n $CONDA_ENV -c conda-forge python=3.13
RUN micromamba install -n $CONDA_ENV -f /src/environment.yml
RUN micromamba clean -p -t -l --trash -y
RUN micromamba run -n $CONDA_ENV pip uninstall -y xhtml2pdf && micromamba run -n $CONDA_ENV pip install xhtml2pdf

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
