# Builds an image for CTS calculator celery worker

#FROM python:2.7
FROM puruckertom/uber_py27

# Install requirements for cts_celery
COPY requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt

# Copy the project code
COPY . /src/
WORKDIR /src

# ENTRYPOINT ["celery worker -A tasks -Q epi -l info -n epi_worker -c 2"]