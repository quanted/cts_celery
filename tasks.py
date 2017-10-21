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
import json


logging.getLogger('celery.task.default').setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)


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



from cts_calcs.calculator_chemaxon import JchemCalc
from cts_calcs.calculator_sparc import SparcCalc
from cts_calcs.calculator_epi import EpiCalc
from cts_calcs.calculator_measured import MeasuredCalc
from cts_calcs.calculator_test import TestCalc
from cts_calcs.calculator_test import TestWSCalc
from cts_calcs.calculator_metabolizer import MetabolizerCalc
from cts_calcs.calculator import Calculator
from cts_calcs import smilesfilter



REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME')

if not os.environ.get('REDIS_HOSTNAME'):
    os.environ.setdefault('REDIS_HOSTNAME', 'localhost')
    REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME')

logging.info("REDIS HOSTNAME: {}".format(REDIS_HOSTNAME))



# redis_conn = redis.StrictRedis(host=REDIS_HOSTNAME, port=6379, db=0)

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
def chemaxonTask(request_post):
    try:
        logging.info("celery worker consuming chemaxon task")
        # return JchemCalc().data_request_handler(request_post, True)
        _results = JchemCalc().data_request_handler(request_post, True)
        Calculator().redis_conn.publish(request_post.get('sessionid'), json.dumps(_results))
    except KeyError as ke:
        logging.warning("exception in calcTask: {}".format(ke))
        raise KeyError("Request to calc task needs 'calc' and 'service' keys")


@app.task
def sparcTask(request_post):
    try:
        logging.info("celery worker consuming sparc task")
        _results = SparcCalc().data_request_handler(request_post)
        Calculator().redis_conn.publish(request_post.get('sessionid'), json.dumps(_results))
    except KeyError as ke:
        logging.warning("exception in calcTask: {}".format(ke))
        raise KeyError("Request to calc task needs 'calc' and 'service' keys")


@app.task
def epiTask(request_post):
    """
    NOTE: EPI water solubility request now returns two values.
    There are many ways to parse these, from cts_pchemprop_requests.html to
    calculator_epi.py. Unlike chemaxon's kow that's 1 call / method, epi
    returns both methods in one call, so I think it's best in terms of only
    having to change code in one place, to loop the methods here and push
    them separately to the front with their on 'method' key:val, which the frontend
    should hopefully handle it like chemaxon's kow...
    """
    try:
        logging.info("celery worker consuming epi task")
        _results = EpiCalc().data_request_handler(request_post)

        if request_post.get('prop') == 'water_sol':
            # _result schema for ws: {'data': {'data': [{}, {}]}}
            for _data_obj in _results['data']['data']:
                _data_obj['calc'] = "epi"  # add calc name for frontend
                Calculator().redis_conn.publish(request_post.get('sessionid'), json.dumps(_data_obj))
        else:
            Calculator().redis_conn.publish(request_post.get('sessionid'), json.dumps(_results))

    except KeyError as ke:
        logging.warning("exception in calcTask: {}".format(ke))
        raise KeyError("Request to calc task needs 'calc' and 'service' keys")


@app.task
def testTask(request_post):
    try:
        logging.info("celery worker consuming TEST task")
        _results = TestCalc().data_request_handler(request_post)
        Calculator().redis_conn.publish(request_post.get('sessionid'), json.dumps(_results))
    except KeyError as ke:
        logging.warning("exception in calcTask: {}".format(ke))
        raise KeyError("Request to calc task needs 'calc' and 'service' keys")


@app.task
def testWSTask(request_post):
    try:
        logging.info("celery worker consuming TEST WS task")
        _results = TestWSCalc().data_request_handler(request_post)
        Calculator().redis_conn.publish(request_post.get('sessionid'), json.dumps(_results))
    except KeyError as ke:
        logging.warning("exception in calcTask: {}".format(ke))
        raise KeyError("Request to calc task needs 'calc' and 'service' keys")


@app.task
def measuredTask(request_post):
    try:
        logging.info("celery worker consuming measured task")
        _results = MeasuredCalc().data_request_handler(request_post)
        Calculator().redis_conn.publish(request_post.get('sessionid'), json.dumps(_results))
        return _results
    except KeyError as ke:
        logging.warning("exception in calcTask: {}".format(ke))
        raise KeyError("Request to calc task needs 'calc' and 'service' keys")

@app.task
def metabolizerTask(request_post):
    # try:
    logging.info("celery worker consuming metabolizer task")
    _results = MetabolizerCalc().data_request_handler(request_post)
    logging.warning("PUSHING BACK TO CLIENT: {} ~~~{} ~~~".format(_results, request_post.get('sessionid')))
    Calculator().redis_conn.publish(request_post.get('sessionid'), json.dumps(_results))
    # except KeyError as ke:
    #     logging.warning("exception in calcTask: {}".format(ke))
    #     raise KeyError("Request to calc task needs 'calc' and 'service' keys")


@app.task
def chemInfoTask(request_post):
    """
    A websocket version /cts/rest/molecule endpoint
    """

    _chem_keys = ['chemical','orig_smiles','smiles','formula','iupac','cas','mass','structureData','exactMass']

    logging.info("celery worker consuming chem info task")
    chemical = request_post.get('chemical')
    get_sd = request_post.get('get_structure_data')  # bool for getting <cml> format image for marvin sketch

    # try:

    response = Calculator().convertToSMILES({'chemical': chemical})
    orig_smiles = response['structure']
    filtered_smiles_response = smilesfilter.filterSMILES(orig_smiles)
    filtered_smiles = filtered_smiles_response['results'][-1]

    logging.warning("Filtered SMILES: {}".format(filtered_smiles))

    jchem_response = Calculator().getChemDetails({'chemical': filtered_smiles})  # get chemical details

    # molecule_obj = Molecule().createMolecule(chemical, orig_smiles, jchem_response, get_sd)
    # chem_list = []
    # for chem_info_dict in jchem_response['data']:
    molecule_obj = {'chemical': filtered_smiles}
    for key, val in jchem_response['data'][0].items():
        molecule_obj[key] = val
        # chem_list.append(molecule_obj)

    if request_post.get('is_node'):
        #### only get these if gentrans single mode: ####
        molecule_obj.update({'node_image': Calculator().nodeWrapper(filtered_smiles, MetabolizerCalc().tree_image_height, MetabolizerCalc().tree_image_width, MetabolizerCalc().image_scale, MetabolizerCalc().metID,'svg', True)})
        molecule_obj.update({
            'popup_image': Calculator().popupBuilder(
                {"smiles": filtered_smiles}, 
                MetabolizerCalc().metabolite_keys, 
                "{}".format(request_post.get('id')),
                "Metabolite Information")
        })
        ##################################################

    wrapped_post = {
        'status': True,  # 'metadata': '',
        'data': molecule_obj,
        'request_post': request_post
    }
    json_data = json.dumps(wrapped_post)

    logging.warning("Returning Chemical Info: {}".format(json_data))

    Calculator().redis_conn.publish(request_post.get('sessionid'), json_data)

    return wrapped_post
    

@app.task
def removeUserJobsFromQueue(sessionid):
    logging.info("clearing celery task queues from user {}..".format(sessionid))
    removeUserJobsFromQueue(sessionid)  # clear jobs from celery
    logging.info("clearing redis cache from user {}..".format(sessionid))
    removeUserJobsFromRedis(sessionid)  # clear jobs from redis


@app.task
def test_celery(sessionid, message):
    logging.info("!!!received message: {}".format(message))
    Calculator().redis_conn.publish(sessionid, "hello from celery")  # async push to user


def removeUserJobsFromRedis(sessionid):
    try:
        user_jobs_json = Calculator().redis_conn.get(sessionid)  # all user's jobs

        logging.info("user's jobs: {}".format(user_jobs_json))
        
        if user_jobs_json:
            Calculator().redis_conn.delete(sessionid)

        return True
        
    except Exception as e:
        raise e


def removeUserJobsFromQueue(sessionid):
    from celery.task.control import revoke

    user_jobs_json = Calculator().redis_conn.get(sessionid)
    logging.info("JOBS: {}".format(user_jobs_json))

    if not user_jobs_json:
        logging.info("no user jobs, moving on..")
        return

    user_jobs = json.loads(user_jobs_json)
    for job_id in user_jobs['jobs']:
        logging.info("revoking job {}".format(job_id))
        revoke(job_id, terminate=True)  # stop user job
        logging.info("revoked {} job".format(job_id))

    Calculator().redis_conn.publish(sessionid, json.dumps({'status': "p-chem data request canceled"}))