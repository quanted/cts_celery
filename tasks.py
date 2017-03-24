"""
CTS celery instance
"""
from __future__ import absolute_import
import os
from os.path import dirname, abspath
import sys
from celery import Celery
import logging
import redis


logging.getLogger('celery.task.default').setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)



# imports URLs for cts_calcs below using git-ignored settings_local.py files
# try:
#     # from . import settings_local
#     # import settings_local
#     from settings_local import *
#     logging.info("Imported local settings!")
# except ImportError:
#     logging.info("Could not import settings_local in celery_cts")
#     pass


# This is where the above should be removed, and instead
# the set_environment.py module could be ran to set env vars
# from the config/ env vars files.
# BUT, can the module be accessed from the parent dir???
# from qed_cts.set_environment import DeployEnv
from temp_config.set_environment import DeployEnv
runtime_env = DeployEnv()
runtime_env.load_deployment_environment()


# from django.conf import settings
# settings.configure()
if not os.environ.get('DJANGO_SETTINGS_FILE'):
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'qed_cts.settings_outside')
else:
    # os.environ.setdefault('DJANGO_SETTINGS_MODULE', '.' + os.environ.get('DJANGO_SETTINGS_FILE'))
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')



# from cts_calcs.chemaxon_cts import worker as chemaxon_worker
from cts_calcs.sparc_cts import worker as sparc_worker
# from cts_calcs.epi_cts import worker as epi_worker
# from cts_calcs.test_cts import worker as test_worker
# from cts_calcs.measured_cts import worker as measured_worker
from cts_calcs.calculator_chemaxon import ChemaxonCalc

from cts_calcs.calculator import Calculator
# from calculator import Calculator


REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME')

if not os.environ.get('REDIS_HOSTNAME'):
    os.environ.setdefault('REDIS_HOSTNAME', 'localhost')
    REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME')

logging.info("REDIS HOSTNAME: {}".format(REDIS_HOSTNAME))

redis_conn = redis.StrictRedis(host=REDIS_HOSTNAME, port=6379, db=0)

app = Celery('tasks',
				broker='redis://{}:6379/0'.format(REDIS_HOSTNAME),	
				backend='redis://{}:6379/0'.format(REDIS_HOSTNAME))

app.conf.update(
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json',
)



##### THE TASKS #####

@app.task
def calcTask(request_post):
    """
    General calculator server request task
    """
    request = NotDjangoRequest()
    request.POST = request_post
    logging.info("Request consumed by calcTask: {}".format(request_post))
    return ChemaxonCalc().data_request_handler(request_post)

# @app.task
# def chemaxonTask(request_post):
#     request = NotDjangoRequest()
#     request.POST = request_post
#     logging.info("Request: {}".format(request_post))
#     return chemaxon_worker.request_manager(request)


@app.task
def sparcTask(request_post):
    request = NotDjangoRequest()
    request.POST = request_post
    return sparc_worker.request_manager(request)


# @app.task
# def epiTask(request_post):
#     request = NotDjangoRequest()
#     request.POST = request_post
#     return epi_worker.request_manager(request)


# @app.task
# def testTask(request_post):
#     request = NotDjangoRequest()
#     request.POST = request_post
#     return test_worker.request_manager(request)


# @app.task
# def measuredTask(request_post):
#     request = NotDjangoRequest()
#     request.POST = request_post
#     return measured_worker.request_manager(request)


# @shared_task
@app.task
def removeUserJobsFromQueue(sessionid):
    logging.info("clearing celery task queues..")
    removeUserJobsFromQueue(sessionid)  # clear jobs from celery
    logging.info("clearing redis cache..")
    removeUserJobsFromRedis(sessionid)  # clear jobs from redis


@app.task
def test_celery(sessionid, message):
    logging.info("!!!received message: {}".format(message))
    redis_conn.publish(sessionid, "hello from celery")  # async push to user


# patch for freeing celery from django while calc views
# are still relying on django.http Request...
class NotDjangoRequest(object):
    def __init__(self):
        self.POST = {}


def removeUserJobsFromRedis(sessionid):
    try:
        user_jobs_json = redis_conn.get(sessionid)  # all user's jobs

        logging.info("user's jobs: {}".format(user_jobs_json))
        
        if user_jobs_json:
            redis_conn.delete(sessionid)

        return True
        
    except Exception as e:
        raise e


def removeUserJobsFromQueue(sessionid):
    from celery.task.control import revoke

    user_jobs_json = redis_conn.get(sessionid)
    logging.info("JOBS: {}".format(user_jobs_json))

    if not user_jobs_json:
        logging.info("no user jobs, moving on..")
        return

    user_jobs = json.loads(user_jobs_json)
    for job_id in user_jobs['jobs']:
        logging.info("revoking job {}".format(job_id))
        revoke(job_id, terminate=True)  # stop user job
        logging.info("revoked {} job".format(job_id))

    redis_conn.publish(sessionid, json.dumps({'status': "p-chem data request canceled"}))

