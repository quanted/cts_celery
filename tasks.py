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
import numpy as np

logging.getLogger('celery.task.default').setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)

from temp_config.set_environment import DeployEnv
runtime_env = DeployEnv()
runtime_env.load_deployment_environment()

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')  # loads faux django settings so celery can use django lib for templating

from cts_calcs.calculator_chemaxon import JchemCalc
from cts_calcs.calculator_sparc import SparcCalc
from cts_calcs.calculator_epi import EpiCalc
from cts_calcs.calculator_measured import MeasuredCalc
from cts_calcs.calculator_test import TestWSCalc
from cts_calcs.calculator_metabolizer import MetabolizerCalc
from cts_calcs.calculator_biotrans import BiotransCalc
from cts_calcs.calculator_opera import OperaCalc
from cts_calcs.calculator import Calculator
from cts_calcs.chemical_information import ChemInfo
from cts_calcs.mongodb_handler import MongoDBHandler
from celery.task.control import revoke



db_handler = MongoDBHandler()  # mongodb handler for opera pchem data

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
		task_obj = CTSTasks()
		task_obj.initiate_requests_parsing(request_post)
	except Exception as e:
		logging.warning("Error calling task: {}".format(e))
		if db_handler.is_connected:
			db_handler.mongodb_conn.close()  # closes mongodb client connection
		task_obj.build_error_obj(request_post, 'cannot reach calculator', e)  # generic error

@app.task
def removeUserJobsFromQueue(sessionid):
	_task_obj = QEDTasks()
	_task_obj.revoke_queued_jobs(sessionid)  # clear jobs from celery
	_task_obj.remove_redis_jobs(sessionid)  # clear jobs from redis

@app.task
def test_celery(sessionid, message):
	logging.info("test_celery received message: {}".format(message))
	Calculator().redis_conn.publish(sessionid, "hello from celery")  # async push to user



##################################################################
########## App classes used by the celery tasks ##################
##################################################################

class QEDTasks:
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
			user_jobs_list = self.redis_conn.lrange(sessionid, 0, -1)
			if user_jobs_list:
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
		user_jobs_list = self.redis_conn.lrange(sessionid, 0, -1)
		if not user_jobs_list:
			return
		for job_id in user_jobs_list:
			revoke(job_id, terminate=True)  # stop user job



class CTSTasks(QEDTasks):
	"""
	General class for cts tasks to call,
	and keeping tasks as single-level functions
	for now. Seems like the Task class has changed
	from celery 3 to 4 (http://docs.celeryproject.org/en/latest/whatsnew-4.0.html)
	"""
	def __init__(self):
		QEDTasks.__init__(self)
		self.chemaxon_calc = JchemCalc()
		self.epi_calc = EpiCalc()
		self.testws_calc = TestWSCalc()
		self.sparc_calc = SparcCalc()
		self.measured_calc = MeasuredCalc()
		self.metabolizer_calc = MetabolizerCalc()
		self.biotrans_calc = BiotransCalc()
		self.chem_info_obj = ChemInfo()
		self.opera_calc = OperaCalc()

	def build_list_of_chems(self, request_post):
		"""
		Builds list of chemicals from 'nodes'.
		"""
		if not 'nodes' in request_post:
			return
		chem_list = []
		for node in request_post['nodes']:
			chem_list.append(node['smiles'])
		return chem_list

	def create_response_obj(self, collection_type, request_post, db_results):
		"""
		Creates response object from mongodb results.
		"""
		if collection_type == 'chem_info':
			response_obj = dict(self.chem_info_obj.wrapped_post)
			response_obj['status'] = True
			response_obj['data'] = db_results
			response_obj['request_post'] = request_post
			return response_obj

	def build_error_obj(self, response_post, error_message, thrown_error=None):
		if response_post.get('prop'):
			default_error_obj = {
				'chemical': response_post['chemical'],
				'calc': response_post['calc'],
				'prop': response_post['prop'],
				'data': error_message
			}
			self.redis_conn.publish(response_post['sessionid'], json.dumps(default_error_obj))
			return
		if 'props' in response_post and 'pchem_request' in response_post:
			# Loops pchem request list of requested properties for a given calculator:
			for prop in response_post['pchem_request'][response_post['calc']]:
				default_error_obj = dict(response_post)
				default_error_obj.update({
					'chemical': response_post['chemical'],
					'calc': response_post['calc'],
					'prop': prop,
					'data': error_message
				})
				self.redis_conn.publish(response_post['sessionid'], json.dumps(default_error_obj))
			return

		if response_post.get('calc') == 'biotrans':
			if not thrown_error:
				thrown_error = "Cannot retrieve data"
			self.redis_conn.publish(response_post['sessionid'], json.dumps({'error': str(thrown_error)}))


	def initiate_requests_parsing(self, request_post):
		"""
		Checks if request is single chemical or list of chemicals, then 
		parses request up to fill worker queues w/ single chemical requests.
		This was originally structured this way because revoking celery work
		seems to only be successful for jobs not yet started.

		It accounts for the case of a user requesting data for many chemicals
		(single job), then leaving page; the celery workers would continue processing
		that job despite the user not being there :(
		"""
		if 'nodes' in request_post and request_post.get('calc') == 'opera':
			# Send all chemicals to OPERA calc to compute at the same time:
			self.handle_opera_request(request_post.get('sessionid'), request_post, batch=True)
			return
		if 'nodes' in request_post:
			# Handles batch mode one chemical at a time:
			for node in request_post['nodes']:
				request_obj = dict(request_post)
				request_obj['node'] = node
				request_obj['chemical'] = node['smiles']
				request_obj['mass'] = node.get('mass')
				request_obj['request_post'] = {'service': request_post.get('service')}
				del request_obj['nodes']
				self.parse_by_service(request_post.get('sessionid'), request_obj)
		else:
			self.parse_by_service(request_post.get('sessionid'), request_post)

	def parse_by_service(self, sessionid, request_post):
		"""
		Further parsing of user request.
		Checks if 'service', if not it assumes p-chem request
		Output: Returns nothing, pushes to redis (may not stay this way)
		"""
		request_post['sessionid'] = sessionid
		if request_post.get('service') == 'getSpeciationData':
			_results = self.chemaxon_calc.data_request_handler(request_post)
			self.redis_conn.publish(sessionid, json.dumps(_results))
		elif (request_post.get('service') == 'getTransProducts'):

			if request_post.get('calc') == 'biotrans':
				_results = self.biotrans_calc.data_request_handler(request_post)
			else:
				_results = self.metabolizer_calc.data_request_handler(request_post)
	
			self.redis_conn.publish(sessionid, json.dumps(_results))
		elif (request_post.get('service') == 'getChemInfo'):
			# # chem_info database routine:
			# ############################################################
			# # Gets dsstox id to check if chem info exists in DB:
			# dsstox_result = self.chem_info_obj.get_cheminfo(request_post, only_dsstox=True)
			# db_results = db_handler.find_chem_info_document({'dsstoxSubstanceId': dsstox_result.get('dsstoxSubstanceId')})
			# if db_results:
			# 	logging.info("Getting chem info from DB.")
			# 	del db_results['_id']
			# 	_results = self.create_response_obj('chem_info', request_post, db_results)
			# else:
			# 	logging.info("Making request for chem info.")
			# 	_results = self.chem_info_obj.get_cheminfo(request_post)  # gets chem info
			# 	db_handler.insert_chem_info_data(_results['data'])  # inserts chem info into db
			# ############################################################
			_results = self.chem_info_obj.get_cheminfo(request_post)  # gets chem info
			self.redis_conn.publish(sessionid, json.dumps(_results))
		else:
			self.parse_pchem_request_by_calc(sessionid, request_post)
		return

	def parse_pchem_request_by_calc(self, sessionid, request_post):
		"""
		This function loops a user's p-chem request and parses
		the work by calculator.
		Output: Returns nothing, pushes to redis (may not stay this way, instead
		the redis pushing may be handled at the task function level).
		"""
		calc = request_post['calc']
		if calc == 'measured':
			self.handle_measured_request(sessionid, request_post)
		elif calc == 'epi':
			self.handle_epi_request(sessionid, request_post)
		elif calc == 'sparc':
			self.handle_sparc_request(sessionid, request_post)
		elif calc == 'test':
			self.handle_testws_request(sessionid, request_post)
		elif calc == 'chemaxon':
			self.handle_chemaxon_request(sessionid, request_post)
		elif calc == 'opera':
			self.handle_opera_request(sessionid, request_post)

	def check_opera_db(self, request_post):
		"""
		Checks to see if OPERA p-chem data is available in DB, returns it
		if it exists, and returns False if not.
		"""
		if not db_handler.is_connected:
			return False
		dsstox_result = self.chem_info_obj.get_cheminfo(request_post, only_dsstox=True)
		if not dsstox_result or dsstox_result.get('dsstoxSubstanceId') == "N/A":
			return False
		dtxcid_result = db_handler.find_dtxcid_document({'DTXSID': dsstox_result.get('dsstoxSubstanceId')})
		db_results = None
		if not dtxcid_result:
			return False
		db_results = db_handler.pchem_collection.find({'dsstoxSubstanceId': dtxcid_result.get('DTXCID')})
		if not db_results:
			return False
		return db_results

	def wrap_db_results(self, chem_data, db_results, requested_props):
		"""
		Wraps a chemical's OPERA p-chem DB results with the key:vals
		needed for the frontend.	
		"""
		chem_data_list = []
		for result in db_results:
			if not result.get('prop') in requested_props:
				continue
			if result.get('prop') == 'ion_con':
				# Converts pka/pkb dict to string:
				result['data'] = 'pKa: ' + result['data'].get('pKa') + '\npKb: ' + result['data'].get('pKb')
			result.update(chem_data)
			result['data'] = self.opera_calc.convert_units_for_cts(result['prop'], result)
			del result['_id']
			chem_data_list.append(result)
		return chem_data_list

	def handle_opera_request(self, sessionid, request_post, batch=False):
		"""
		Handles OPERA calculator p-chem requests. Makes one request and
		gets list of all properties at once.
		"""
		db_handler.connect_to_db()
		pchem_data = {}
		request_post['props'] = request_post['pchem_request']['opera']
		# Removes any props not available for OPERA:
		for prop in request_post['props']:
			if not prop in self.opera_calc.props:
				del request_post[prop]
		# Ignores request if requested props aren't available to OPERA:
		if len(request_post['props']) < 1:
			return
		if batch:
			chems = self.build_list_of_chems(request_post)
			pchem_data['data'] = []
			node_index = 0
			remaining_chems = []  # list of chems not found in db
			for chemical_obj in request_post['nodes']:
				chem_data = dict(request_post)
				chem_data.update(chemical_obj)
				chem_data['node'] = request_post['nodes'][node_index]
				chem_data['request_post'] = {'workflow': request_post.get('workflow')}
				del chem_data['nodes']
				db_results = self.check_opera_db(chem_data)
				if not db_results:
					remaining_chems.append(chemical_obj['smiles'])
					continue
				wrapped_results = self.wrap_db_results(chem_data, db_results, request_post['props'])
				wrapped_results = self.opera_calc.remove_opera_db_duplicates(wrapped_results)
				pchem_data['data'] += wrapped_results
				node_index += 1
			# Runs OPERA model for list of remaining chemicals not in DB:
			if len(remaining_chems) > 0:
				request_post['chemical'] = remaining_chems
				model_data = self.opera_calc.data_request_handler(request_post)
				pchem_data['data'] += model_data.get('data')
			pchem_data['valid'] = True
		else:
			db_results = self.check_opera_db(request_post)  # checks db for pchem data
			if not db_results:
				pchem_data = self.opera_calc.data_request_handler(request_post)
			else:
				pchem_data = {'valid': True, 'request_post': request_post, 'data': []}
				pchem_data['data'] = self.wrap_db_results(request_post, db_results, request_post.get('props'))
				pchem_data['data'] = self.opera_calc.remove_opera_db_duplicates(pchem_data['data'])
		if not pchem_data.get('valid'):
			self.build_error_obj(pchem_data, pchem_data.get('data'))
			return
		# Returns pchem data 1 prop at a time:
		for pchem_datum in pchem_data.get('data'):
			self.redis_conn.publish(sessionid, json.dumps(pchem_datum))
		db_handler.mongodb_conn.close()

	def handle_chemaxon_request(self, sessionid, request_post):
		"""
		Handles ChemAxon calculator p-chem requests, one p-chem
		property at a time.
		"""
		props = request_post['pchem_request']['chemaxon']
		# Makes a request per property (and per method if they exist)
		for prop_index in range(0, len(props)):
			# Sets 'prop' key for making request to calculator:
			prop = props[prop_index]
			request_post['prop'] = prop
			is_kow = prop == 'kow_no_ph' or prop == 'kow_wph'  # kow has 3 methods
			if is_kow:
				# Making request for each method (required by jchem ws):
				for i in range(0, len(self.chemaxon_calc.methods)):
					request_post['method'] = self.chemaxon_calc.methods[i]
					_results = self.chemaxon_calc.data_request_handler(request_post)
					self.redis_conn.publish(sessionid, json.dumps(_results))
			else:
				# All other chemaxon properties have single method:
				_results = self.chemaxon_calc.data_request_handler(request_post)
				self.redis_conn.publish(sessionid, json.dumps(_results))

	def handle_sparc_request(self, sessionid, request_post):
		"""
		Handles SPARC calculator p-chem requests, one p-chem
		property at a time.
		"""
		props = request_post['pchem_request']['sparc']
		if 'kow_wph' in props:
			request_post['prop'] = 'kow_wph'
			_results = self.sparc_calc.data_request_handler(request_post)			
			self.redis_conn.publish(sessionid, json.dumps(_results))
		if 'ion_con' in props:
			request_post['prop'] = 'ion_con'
			_results = self.sparc_calc.data_request_handler(request_post)			
			self.redis_conn.publish(sessionid, json.dumps(_results))
		# Handles multiprop response (all props but ion_con or kow_wph):
		request_post['prop'] = "multi"  # gets remaining props from SPARC multiprop endpoint
		_results = self.sparc_calc.data_request_handler(request_post)
		for prop_obj in _results:
			if prop_obj['prop'] in props and not prop_obj['prop'] in ['ion_con', 'kow_wph']:
				# Wrap SPARC datum with request_post keys for frontend:
				prop_obj.update({'request_post': {'service': None}})
				request_post['prop'] = prop_obj['prop']
				prop_obj.update(request_post)
				# Returns user-requsted result prop:
				self.redis_conn.publish(sessionid, json.dumps(prop_obj))

	def handle_testws_request(self, sessionid, request_post):
		"""
		Handles TESTWS calculator p-chem requests, which have 3
		methods for each property, except log_bcf which has 4 methods.
		"""
		props = request_post['pchem_request']['test']
		# Makes a request per property (and per method if they exist)
		for prop_index in range(0, len(props)):
			# Sets 'prop' key for making request to calculator:
			prop = props[prop_index]
			request_post['prop'] = prop
			if prop == 'log_bcf':
				# Adds additional SM method for log_bcf property:
				request_post['method'] = self.testws_calc.bcf_method
				_results = self.testws_calc.data_request_handler(request_post)
				self.redis_conn.publish(sessionid, json.dumps(_results))
			for method in self.testws_calc.methods:
				# Makes requests for each TESTWS method available:
				request_post['method'] = method
				_results = self.testws_calc.data_request_handler(request_post)  # Using TESTWS instead of in-house TEST model
				self.redis_conn.publish(sessionid, json.dumps(_results))

	def handle_measured_request(self, sessionid, request_post):
		"""
		Handles Measured calculator p-chem requests. For Measured,
		a single request returns data for all properties, and some of
		those properties have methods.
		"""
		_results = self.measured_calc.data_request_handler(request_post)
		_results['calc'] == "measured"
		props = request_post['pchem_request']['measured']  # requested properties for measured
		_returned_props = []  # keeping track of any missing prop data that was requested
		# check if results are valid:
		if not _results.get('valid'):
			# not valid, send error message in place of data for requested props..
			for measured_prop in request_post.get('pchem_request', {}).get('measured', []):
				_results['prop'] = measured_prop
				self.redis_conn.publish(sessionid, json.dumps(_results))
			return
		if 'error' in _results:
			for prop in props:
				_results.update({'prop': prop, 'data': _results.get('error')})
				self.redis_conn.publish(sessionid, json.dumps(_results))
			return
		for _data_obj in _results.get('data'):
			for prop in props:
				# looping user-selected props (cts named props):
				if _data_obj['prop'] == self.measured_calc.propMap[prop]['result_key']:
					_results.update({'prop': prop, 'data': _data_obj.get('data')})
					self.redis_conn.publish(sessionid, json.dumps(_results))
					_returned_props.append(prop)
		# Check for any missing prop data that user requested..
		_diff_set = set(_returned_props)^set(props)
		for missing_prop in _diff_set:
			_results.update({'prop': missing_prop, 'data': "N/A"})
			self.redis_conn.publish(sessionid, json.dumps(_results))  # push up as "N/A"

	def handle_epi_request(self, sessionid, request_post):
		"""
		Handles EPI calculator p-chem requests. For EPI,
		a single request returns data for all properties, and
		some of those properties have multiple methods.
		"""
		epi_props_list = request_post.get('pchem_request', {}).get('epi', [])  # available epi props
		props = request_post['pchem_request']['epi']  # user's requested epi props
		if 'water_sol' in epi_props_list or 'vapor_press' in epi_props_list:
			request_post['prop'] = 'water_sol'  # trigger cts epi calc to get MP for epi request
		_results = self.epi_calc.data_request_handler(request_post)
		_response_info = {}
		# check if results are valid:
		if not _results.get('valid'):
			# not valid, send error message in place of data for requested props..
			for epi_prop in request_post.get('pchem_request', {}).get('epi', []):
				_results['prop'] = epi_prop
				if 'methods' in self.epi_calc.propMap[epi_prop]:
					# Sends a response for each method, if the prop has any:
					for key, val in self.epi_calc.propMap[epi_prop]['methods'].items():
						self.redis_conn.publish(sessionid, json.dumps(_results))
				else:
					self.redis_conn.publish(sessionid, json.dumps(_results))
			return
		# key:vals to add to response data objects:
		for key, val in _results.items():
			if not key == 'data':
				_response_info[key] = val
		if not isinstance(_results.get('data'), list):
			self.redis_conn.publish(sessionid, json.dumps({
				'data': "Cannot reach EPI",
				'prop': _response_info.get('prop'),
				'calc': "epi"})
			)
			return
		for _data_obj in _results.get('data', []):
			_epi_prop = _data_obj.get('prop')
			_cts_prop_name = self.epi_calc.props[self.epi_calc.epi_props.index(_epi_prop)] # map epi ws key to cts prop key
			_method = _data_obj.get('method')
			if _method:
				# Use abbreviated method name for pchem table:
				_epi_methods = self.epi_calc.propMap.get(_cts_prop_name).get('methods', {})
				_method = _epi_methods.get(_data_obj['method'])  # use pchem table name for method
			if _cts_prop_name in props:
				_data_obj.update(_response_info)  # data obj going to client needs some extra keys
				_data_obj['prop'] = _cts_prop_name
				_data_obj['method'] = _method
				self.redis_conn.publish(sessionid, json.dumps(_data_obj))