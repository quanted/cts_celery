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


from temp_config.set_environment import DeployEnv
runtime_env = DeployEnv()
runtime_env.load_deployment_environment()


# from django.conf import settings
# settings.configure()
if not os.environ.get('DJANGO_SETTINGS_FILE'):
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'qed_cts.settings_outside')
else:
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



##### THE TASKS #########################################################   

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


@app.task
def cts_task(request_post):

    logging.info("Request post coming into cts_task: {}".format(request_post))

    if 'nodes' in request_post:
        for node in request_post['nodes']:
            request_post['node'] = node
            request_post['chemical'] = node['smiles']
            request_post['mass'] = node['mass']
            # jobID = requestHandler(sessionid, request_post, client)
            jobID = requestHandler(request_post.get('sessionid'), request_post)
    else:
        # jobID = requestHandler(sessionid, request_post, client)
        jobID = requestHandler(request_post.get('sessionid'), request_post)


##################################################################



def getChemInfo(request_post):
    """
    A websocket version of /cts/rest/molecule endpoint.

    NOTE: Currently doesn't use the actorws services, like
    cts_rest.py does for the Chemical Editor.
    """

    logging.info("celery worker consuming chem info task")
    chemical = request_post.get('chemical')
    get_sd = request_post.get('get_structure_data')  # bool for getting <cml> format image for marvin sketch

    calc_obj = Calculator()

    response = calc_obj.convertToSMILES({'chemical': chemical})
    orig_smiles = response['structure']
    filtered_smiles_response = smilesfilter.filterSMILES(orig_smiles)
    filtered_smiles = filtered_smiles_response['results'][-1]

    logging.info("Filtered SMILES: {}".format(filtered_smiles))

    jchem_response = calc_obj.getChemDetails({'chemical': filtered_smiles})  # get chemical details

    molecule_obj = {'chemical': filtered_smiles}
    for key, val in jchem_response['data'][0].items():
        molecule_obj[key] = val
        # chem_list.append(molecule_obj)

    if request_post.get('is_node'):
        #### only get these if gentrans single mode: ####
        molecule_obj.update({'node_image': calc_obj.nodeWrapper(filtered_smiles, MetabolizerCalc().tree_image_height, MetabolizerCalc().tree_image_width, MetabolizerCalc().image_scale, MetabolizerCalc().metID,'svg', True)})
        molecule_obj.update({
            'popup_image': calc_obj.popupBuilder(
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

    logging.info("Returning Chemical Info: {}".format(json_data))

    return wrapped_post



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
    logging.info("{} JOBS: {}".format(sessionid, user_jobs_json))

    if not user_jobs_json:
        logging.info("no user jobs, moving on..")
        return

    user_jobs = json.loads(user_jobs_json)
    for job_id in user_jobs['jobs']:
        logging.info("revoking job {}".format(job_id))
        revoke(job_id, terminate=True)  # stop user job
        logging.info("revoked {} job".format(job_id))

    Calculator().redis_conn.publish(sessionid, json.dumps({'status': "p-chem data request canceled"}))





# function from node_server.js
# todo: make a celery task class to organize this all better
def requestHandler(sessionid, data_obj):

    data_obj['sessionid'] = sessionid

    if data_obj.get('service') == 'getSpeciationData':
        logging.info("celery worker consuming chemaxon task")
        _results = JchemCalc().data_request_handler(data_obj)
        Calculator().redis_conn.publish(sessionid, json.dumps(_results))

    elif (data_obj.get('service') == 'getTransProducts'):
        logging.info("celery worker consuming metabolizer task")
        _results = MetabolizerCalc().data_request_handler(data_obj)
        Calculator().redis_conn.publish(sessionid, json.dumps(_results))

    elif (data_obj.get('service') == 'getChemInfo'):
        logging.info("celery worker consuming cheminfo task")
        _results = getChemInfo(data_obj)
        Calculator().redis_conn.publish(sessionid, json.dumps(_results))
    else:
        callPchemWorkers(sessionid, data_obj)

    return




def callPchemWorkers(sessionid, data_obj):

    for calc in data_obj['pchem_request']:

        props = data_obj['pchem_request'][calc]
        data_obj['calc'] = calc

        if calc == 'measured':
            logging.info("celery worker consuming measured task")
            measured_calc = MeasuredCalc()
            _results = measured_calc.data_request_handler(data_obj)

            # NOTE: Measured _results is list of all props, so return all user requested
            # prop data one at a time so cts frontend knows what to do!
            # _response_obj = {'calc': calc}
            _results['calc'] == calc  # may already be set..

            for data_obj in _results.get('data'):
                for prop in props:
                    # looping user-selected props (cts named props):
                    if data_obj['prop'] == measured_calc.propMap[prop]['result_key']:
                        # match measured prop names..
                        _results.update({
                            'prop': prop,
                            'data': data_obj['data'] 
                        })
                        Calculator().redis_conn.publish(sessionid, json.dumps(_results))

        else:

            for prop_index in range(0, len(props)):

                prop = props[prop_index]

                data_obj['prop'] = prop

                is_chemaxon = calc == 'chemaxon'
                is_kow = prop == 'kow_no_ph' or prop == 'kow_wph'
                if is_chemaxon and is_kow:

                    chemaxon_calc = JchemCalc()

                    for i in range(0, len(chemaxon_calc.methods)):
                        data_obj['method'] = chemaxon_calc.methods[i]
                        logging.info("celery worker consuming chemaxon task")
                        _results = chemaxon_calc.data_request_handler(data_obj)
                        Calculator().redis_conn.publish(sessionid, json.dumps(_results))

                else:

                    if calc == 'chemaxon':
                        logging.info("celery worker consuming chemaxon task")
                        # _results = JchemCalc().data_request_handler(data_obj)
                        _results = JchemCalc().data_request_handler(data_obj)
                        Calculator().redis_conn.publish(sessionid, json.dumps(_results))

                    elif calc == 'sparc':
                        logging.info("celery worker consuming sparc task")
                        _results = SparcCalc().data_request_handler(data_obj)
                        Calculator().redis_conn.publish(sessionid, json.dumps(_results))

                    elif calc == 'epi':
                        logging.info("celery worker consuming epi task")
                        _results = EpiCalc().data_request_handler(data_obj)

                        logging.info("EPI RESULTS: {}".format(_results))

                        # if data_obj.get('prop') == 'water_sol':
                            # _result schema for ws: {'data': {'data': [{}, {}]}}
                        for _data_obj in _results['data']['data']:
                            _data_obj['prop'] = "water_sol"  # make sure water sol is key frontend expects
                            _data_obj.update(data_obj)  # add request key:vals to result
                            Calculator().redis_conn.publish(sessionid, json.dumps(_data_obj))
                        # else:
                        #     Calculator().redis_conn.publish(sessionid, json.dumps(_results))
   
                    elif calc == 'test':
                        logging.info("celery worker consuming TEST task")
                        _results = TestCalc().data_request_handler(data_obj)
                        
                        Calculator().redis_conn.publish(sessionid, json.dumps(_results))
                    # elif calc == 'testws':
                    #     client.call('tasks.testWSTask', [data_obj])

                    # elif (calc == 'measured'):
                    #     client.call('tasks.measuredTask', [data_obj])   