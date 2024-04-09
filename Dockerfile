# Builds an image for CTS calculator celery worker

FROM mambaorg/micromamba:1.5.8-alpine3.19

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
    py3-lxml \
    py3-pillow \
    openldap-dev \
    python3-dev \
    linux-headers \
    && rm -rf /var/cache/apk/*

COPY . /src/
WORKDIR /src

RUN micromamba create -n $CONDA_ENV -c conda-forge python=3.10
RUN micromamba install -n $CONDA_ENV -f /src/environment.yml
RUN micromamba clean -p -t -l --trash -y
RUN micromamba run -n $CONDA_ENV pip uninstall -y xhtml2pdf && micromamba run -n $CONDA_ENV pip install xhtml2pdf

# # Removes any trace of pip to resolve an open CVE:
# RUN rm -rf \
#     /root/.cache/pip \
#     /usr/local/bin/pip \
#     /usr/local/bin/pip3.10 \
#     /usr/local/bin/pip3 \
#     /usr/local/lib/python3.10/site-packages/pip \
#     /usr/local/lib/python3.10/site-packages/pip-23.0.1.dist-info

# Security Issues Mitigations
# ------------------------- #
# RUN apk del gfortran
# RUN rm -R /opt/conda/pkgs/redis*
# #RUN rm -R /opt/conda/bin/redis*
# RUN rm -R /opt/conda/pkgs/postgres*
# #RUN rm -R /opt/conda/bin/postgres*
RUN find /opt/conda/pkgs/future* -name "*.pem" -delete || true
RUN find /opt/conda/lib/python3.10/site-packages/future -name "*.pem" -delete || true
RUN find /opt/conda/envs/pyenv -name "*.pem" -delete || true
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
