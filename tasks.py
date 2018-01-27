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

from cts_calcs.calculator_chemaxon import JchemCalc
from cts_calcs.calculator_sparc import SparcCalc
from cts_calcs.calculator_epi import EpiCalc
from cts_calcs.calculator_measured import MeasuredCalc
from cts_calcs.calculator_test import TestCalc
from cts_calcs.calculator_test import TestWSCalc
from cts_calcs.calculator_metabolizer import MetabolizerCalc
from cts_calcs.calculator import Calculator
from cts_calcs.chemical_information import ChemInfo
from celery.task.control import revoke



REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)

logging.info("REDIS_HOSTNAME: {}".format(REDIS_HOSTNAME))
logging.info("REDIS_PORT: {}".format(REDIS_PORT))

app = Celery('tasks',
				broker='redis://{}:6379/0'.format(REDIS_HOSTNAME),	
				backend='redis://{}:6379/0'.format(REDIS_HOSTNAME))

app.conf.update(
	CELERY_ACCEPT_CONTENT=['json'],
	CELERY_TASK_SERIALIZER='json',
	CELERY_RESULT_SERIALIZER='json',
)



##################################################################
##### THE TASKS ##################################################
##################################################################

@app.task
def cts_task(request_post):
	try:
		cts_task = CTSTasks()
		sessionid = request_post.get('sessionid')
		results = cts_task.send_request_to_calcs(sessionid, request_post)
		cts_task.publish_results_to_redis(sessionid, results)
	except Exception as e:
		logging.warning("Error in cts_task: {}".format(e))
		results = cts_task.build_error_obj(request_post, 'error')  # generic error
		cts_task.redis_conn.publish(sessionid, json.dumps(results))

@app.task
def removeUserJobsFromQueue(sessionid):
	_task_obj = QEDTasks()
	logging.info("clearing celery task queues from user {}..".format(sessionid))
	_task_obj.revoke_queued_jobs(sessionid)  # clear jobs from celery
	logging.info("clearing redis cache from user {}..".format(sessionid))
	_task_obj.remove_redis_jobs(sessionid)  # clear jobs from redis


@app.task
def test_celery(sessionid, message):
	logging.info("!!!received message: {}".format(message))
	Calculator().redis_conn.publish(sessionid, "hello from celery")  # async push to user



##################################################################
########## App classes used by the celery tasks ##################
##################################################################

class QEDTasks(object):
	"""
	Suggested main class for task related things.
	Anything similar across all apps for task handling
	could go here.

	NOTE: Current setup requires redis host and port to instantiate
	"""
	def __init__(self):
		REDIS_HOSTNAME = os.environ.get('REDIS_HOSTNAME', 'localhost')
		REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
		self.redis_conn = redis.StrictRedis(host=REDIS_HOSTNAME, port=REDIS_PORT, db=0)

	def remove_redis_jobs(self, sessionid):
		"""
		Removes the job IDs stored in redis after
		a user is finished with them
		"""
		try:
			user_jobs_json = self.redis_conn.get(sessionid)  # all user's jobs
			logging.info("user's jobs: {}".format(user_jobs_json))
			if user_jobs_json:
				self.redis_conn.delete(sessionid)  # remove key:vals from user
			return True
		except Exception as e:
			raise e

	def revoke_queued_jobs(self, sessionid):
		"""
		Revokes jobs yet started on the worker queues.
		Happens if user hits "cancel" or leaves page while
		requesting data, and is meant to prevent the queues 
		from clogging up with requests.
		"""
		user_jobs_json = self.redis_conn.get(sessionid)
		logging.info("{} JOBS: {}".format(sessionid, user_jobs_json))
		if not user_jobs_json:
			logging.info("no user jobs, moving on..")
			return
		user_jobs = json.loads(user_jobs_json)
		for job_id in user_jobs['jobs']:
			logging.info("revoking job {}".format(job_id))
			revoke(job_id, terminate=True)  # stop user job
			logging.info("revoked {} job".format(job_id))
		self.redis_conn.publish(sessionid, json.dumps({'status': "p-chem data request canceled"}))




class CTSTasks(QEDTasks):
	"""
	General class for cts tasks to call,
	and keeping tasks as single-level functions
	for now. Seems like the Task class has changed
	from celery 3 to 4 (http://docs.celeryproject.org/en/latest/whatsnew-4.0.html)
	"""
	def __init__(self):
		QEDTasks.__init__(self)
		self.calcs_multi_request = ['chemaxon', 'sparc', 'test', 'cheminfo', 'metabolizer']  # calcs w/ 1 request/prop
		self.calcs_single_request = ['epi', 'measured']  # calcs where 1 request returns all props

	def build_error_obj(self, request_post, error_message):
		default_error_obj = {
			'chemical': request_post['chemical'],
			'calc': request_post['calc'],
			'prop': request_post['prop'],
			'data': error_message
		}
		return default_error_obj

	def send_request_to_calcs(self, sessionid, request_post):
		"""
		Further parsing of user request.
		Checks if 'service', if not it assumes p-chem request
		TODO: at 'pchem' service instead of assuming..

		Output: Returns nothing, pushes to redis (may not stay this way)
		"""
		request_post['sessionid'] = sessionid

		if request_post.get('service') == 'getSpeciationData':
			logging.info("celery worker consuming chemaxon task")
			_results = JchemCalc().data_request_handler(request_post)
			return _results

		elif (request_post.get('service') == 'getTransProducts'):
			logging.info("celery worker consuming metabolizer task")
			_results = MetabolizerCalc().data_request_handler(request_post)
			return _results

		elif (request_post.get('service') == 'getChemInfo'):
			logging.info("celery worker consuming cheminfo task")
			_results = ChemInfo().get_cheminfo(request_post)
			return _results
		else:
			return self.parse_pchem_request(sessionid, request_post)

	def parse_pchem_request(self, sessionid, request_post):
		"""
		Parse request up by calculator.
		Returns calc results.
		"""
		calc = request_post['calc']
		props = request_post['pchem_request'][calc]

		if calc == 'chemaxon':
			return JchemCalc().data_request_handler(request_post)

		elif calc == 'sparc':
			return SparcCalc().data_request_handler(request_post)

		elif calc == 'test':
			return TestCalc().data_request_handler(request_post)

		elif calc == 'measured':
			_results = MeasuredCalc().data_request_handler(request_post)
			_results['calc'] == calc
			return _results

		elif calc == 'epi':
			epi_props_list = request_post.get('pchem_request', {}).get('epi', [])
			if 'water_sol' in epi_props_list or 'vapor_press' in epi_props_list:
				request_post['prop'] = 'water_sol'  # trigger cts epi calc to get MP for epi request
			_results = EpiCalc().data_request_handler(request_post)
			_results['calc'] = calc
			return _results

	def publish_results_to_redis(self, sessionid, results):
		"""
		Sends results to client via publishing to redis, which
		gets picked up by nodejs, which ultimately pushes to client.
		"""
		logging.info("RESULTS: {}".format(results))
		_calc = results.get('calc')

		logging.info("CALC: {}".format(_calc))

		if _calc in self.calcs_multi_request:
			logging.info("Publishing results to redis..")
			self.redis_conn.publish(sessionid, json.dumps(results))  # calcs with 1 request per property..
		elif _calc == 'epi':
			self.parse_and_publish_epi(sessionid, results)
		elif _calc == 'measured':
			self.parse_and_publish_measured(sessionid, results)

	def parse_and_publish_epi(self, sessionid, results):
		"""
		Parse up EPI results and publish them to redis.
		"""
		_response_info = {}

		 # key:vals to add to response data objects:
		for key, val in results.items():
			if not key == 'data':
				_response_info[key] = val

		for _data_obj in results.get('data', []):
			_epi_prop = _data_obj.get('prop')
			_cts_prop_name = epi_calc.props[epi_calc.epi_props.index(_epi_prop)] # map epi ws key to cts prop key

			_method = None
			if _data_obj['prop'] == 'water_solubility':
				_method = _data_obj['method']

			if _cts_prop_name in props:
				_data_obj.update(_response_info)  # data obj going to client needs some extra keys
				_data_obj['prop'] = _cts_prop_name
				
				if _method:
					_data_obj['method'] = _method

				self.redis_conn.publish(sessionid, json.dumps(_data_obj))

	def parse_and_publish_measured(self, sessionid, results):
		"""
		Parses up Measured results and publishes them to redis.
		"""
		_returned_props = []  # keeping track of any missing prop data that was requested

		if 'error' in results:
			for prop in props:
				results.update({'prop': prop, 'data': results.get('error')})
				self.redis_conn.publish(sessionid, json.dumps(results))
			# return

		for _data_obj in results.get('data'):
			for prop in props:
				# looping user-selected props (cts named props):
				if _data_obj['prop'] == measured_calc.propMap[prop]['result_key']:
					results.update({'prop': prop, 'data': _data_obj.get('data')})
					self.redis_conn.publish(sessionid, json.dumps(results))
					_returned_props.append(prop)

		# Check for any missing prop data that user requested..
		_diff_set = set(_returned_props)^set(props)
		for missing_prop in _diff_set:
			logging.warning("{} missing from Measured response..".format(missing_prop))
			results.update({'prop': missing_prop, 'data': "N/A"})
			self.redis_conn.publish(sessionid, json.dumps(results))  # push up as "N/A"