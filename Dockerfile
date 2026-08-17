# Builds an image for CTS calculator celery worker (DHI hardened)
# ---- build stage (has shell, compiler, headers) ----
FROM dhi.io/python:3.13-dev AS build
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libxrender1 \
        libxext6 && \
    rm -rf /var/lib/apt/lists/*

COPY . /src/
WORKDIR /src

RUN pip install --prefix=/install -r requirements.txt

# ---- runtime stage (distroless, nonroot, no shell) ----
FROM dhi.io/python:3.13

COPY --from=build --chown=65532:65532 /install /usr/local
COPY --from=build --chown=65532:65532 /src /src

# RDKit runtime shared libs (same chain as the web image)
COPY --from=build /usr/lib/x86_64-linux-gnu/libXrender.so.1* /usr/lib/x86_64-linux-gnu/
COPY --from=build /usr/lib/x86_64-linux-gnu/libXext.so.6* /usr/lib/x86_64-linux-gnu/
COPY --from=build /usr/lib/x86_64-linux-gnu/libX11.so.6* /usr/lib/x86_64-linux-gnu/
COPY --from=build /usr/lib/x86_64-linux-gnu/libxcb.so.1* /usr/lib/x86_64-linux-gnu/
COPY --from=build /usr/lib/x86_64-linux-gnu/libXau.so.6* /usr/lib/x86_64-linux-gnu/
COPY --from=build /usr/lib/x86_64-linux-gnu/libXdmcp.so.6* /usr/lib/x86_64-linux-gnu/

ENV DJANGO_SETTINGS_MODULE="settings"
ENV PYTHONPATH="/usr/local/lib/python3.13/site-packages:/src:/src"
ENV PATH="/src:${PATH}"
