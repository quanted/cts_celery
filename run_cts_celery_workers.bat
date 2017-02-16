:: Runs celery workers for p-chem calculators for development mode
:: Jun. 2016; np

:: Filename for .env to use when running celery workers:
set env_filename=%1

echo .env Filename
echo env_filename

rem start call ../config/set_env_vars.bat env_filename&&celery -A tasks worker -Q manager --loglevel=info -n manager_worker
:: Runs celery worker with env vars from config/
start call run_worker.bat manager %env_filename%
start call run_worker.bat chemaxon %env_filename%
start call run_worker.bat measured %env_filename%
:: start call ../config/set_env_vars.bat env_filename&&celery -A tasks worker -Q chemaxon --loglevel=info --concurrency=1 -n chemaxon_worker
:: start celery -A tasks worker -Q sparc --loglevel=info --concurrency=1 -n sparc_worker
:: start celery -A tasks worker -Q epi --loglevel=info --concurrency=1 -n epi_worker
:: start celery -A tasks worker -Q measured --loglevel=info --concurrency=1 -n measured_worker
:: start celery -A tasks worker -Q test --loglevel=info --concurrency=1 -n test_worker

:: Below is the little servlet to monitor celery workers. It's not required.
:: start flower -A tasks --port=5000