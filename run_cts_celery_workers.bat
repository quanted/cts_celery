:: Runs celery workers for p-chem calculators for development mode
:: Jun. 2016; np

:: Filename for .env to use when running celery workers:

start celery -A tasks worker -Q manager_queue --loglevel=info --concurrency=1 -n manager_worker
start celery -A tasks worker -Q cts_queue,manager_queue --loglevel=info --concurrency=1 -n cts_worker_1
start celery -A tasks worker -Q cts_queue,manager_queue --loglevel=info --concurrency=1 -n cts_worker_2
start celery -A tasks worker -Q cts_queue,manager_queue --loglevel=info --concurrency=1 -n cts_worker_3