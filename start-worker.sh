#!/bin/bash
celery -A tasks worker -Q cts_queue -l info -n cts_worker -c 4