#!/bin/bash
micromamba run -n pyenv celery -A tasks worker -Q cts_queue -l info -n cts_worker -c 1