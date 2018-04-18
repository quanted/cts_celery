# Builds an image for CTS calculator celery worker

# Use 'version' ARG for grabbing correct qed_py3 base image.
# Defaults to 'latest' if not set.
ARG version=latest
FROM quanted/qed_py3:$version

# Install requirements for cts_celery
# COPY requirements.txt /tmp/
# RUN pip install --requirement /tmp/requirements.txt

# Copy the project code
COPY . /src/
WORKDIR /src
