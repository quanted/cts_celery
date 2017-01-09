"""
CTS celery instance
"""
from __future__ import absolute_import
import os
from celery import Celery
import logging

# imports URLs for cts_calcs below
try:
    # from . import settings_local
    import settings_local
    logging.info("Imported local settings!")
except ImportError:
    logging.info("Could not import settings_local in celery_cts")
    pass

from cts_calcs.chemaxon_cts import worker as chemaxon_worker
from cts_calcs.sparc_cts import worker as sparc_worker
from cts_calcs.epi_cts import worker as epi_worker
from cts_calcs.test_cts import worker as test_worker
from cts_calcs.measured_cts import worker as measured_worker

logging.info("INSIDE CELERY APP")


if not os.environ.get('REDIS_HOSTNAME'):
    os.environ.setdefault('REDIS_HOSTNAME', 'localhost')

REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME')

logging.warning("REDIS HOSTNAME: {}".format(REDIS_HOSTNAME))

app = Celery('tasks',
				broker='redis://{}:6379/0'.format(REDIS_HOSTNAME),	
				backend='redis://{}:6379/0'.format(REDIS_HOSTNAME))

app.conf.update(
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json',
)

logging.getLogger('celery.task.default').setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)



##### THE TASKS #####

@app.task
def chemaxonTask(request_post):
    request = NotDjangoRequest()
    request.POST = request_post
    logging.info("Request: {}".format(request_post))
    return chemaxon_worker.request_manager(request)


@app.task
def sparcTask(request_post):
    request = NotDjangoRequest()
    request.POST = request_post
    return sparc_worker.request_manager(request)


@app.task
def epiTask(request_post):
    request = NotDjangoRequest()
    request.POST = request_post
    return epi_worker.request_manager(request)


@app.task
def testTask(request_post):
    request = NotDjangoRequest()
    request.POST = request_post
    return test_worker.request_manager(request)


@app.task
def measuredTask(request_post):
    request = NotDjangoRequest()
    request.POST = request_post
    return measured_worker.request_manager(request)


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

