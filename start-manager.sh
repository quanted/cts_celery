#!/bin/bash
micromamba run -n pyenv celery -A tasks worker -Q manager_queue -l info -n manager_worker -c 1