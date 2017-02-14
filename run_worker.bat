:: run worker via batch file with calc as input arg with env vars
set calc=%1
set env_filename=%2

echo starting worker, with env_filename
echo %calc%
echo %env_filename%

call ../config/set_env_vars.bat ../config/%env_filename%
celery -A tasks worker -Q %calc% --loglevel=info -n %calc%_worker