#!/bin/sh

xterm -e celery -A tasks worker -Q manager --loglevel=info --concurrency=1 -n manager_worker &
xterm -e celery -A tasks worker -Q chemaxon --loglevel=info --concurrency=1 -n chemaxon_worker &
xterm -e celery -A tasks worker -Q metabolizer --loglevel=info --concurrency=1 -n metabolizer_worker &
xterm -e celery -A tasks worker -Q cheminfo --loglevel=info --concurrency=1 -n cheminfo_worker &
# xterm -hold -e celery -A tasks worker -Q sparc --loglevel=info --concurrency=1 -n sparc_worker
# xterm -hold -e celery -A tasks worker -Q epi --loglevel=info --concurrency=1 -n epi_worker
# xterm -hold -e celery -A tasks worker -Q measured --loglevel=info --concurrency=1 -n measured_worker
# xterm -hold -e celery -A tasks worker -Q test --loglevel=info --concurrency=1 -n test_worker
