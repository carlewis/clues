#! /usr/bin/env python
# coding: utf-8
#
# CLUES - Cluster Energy Saving System
# Copyright (C) 2011 - GRyCAP - Universitat Politecnica de Valencia
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import SocketServer
import string
import threading 
import Queue 
import subprocess
import logging 
import time
import datetime
import os
import evaluation
from optparse import OptionParser
from config import *
from operator import itemgetter
from auth import *
try:
   from config_local import *
except:
   pass
   
# Field constants for node_table
(NAME, STATE, TIME, OFF_WHEN_IDLE, FREE_SLOTS, TOTAL_SLOTS, DICT, REQOBJ) = range(8)
# Node states
(AVAILABLE,    # (Partially) free
 BUSY,
 OFF,
 BOOTING,
 POWOFF,       # Powering off
 FAIL,         # Failed to boot
 FAIL_BOOTING, # Booting but coming from FAIL state
 OFFLINE,      # Marked as OFFLINE by the Resource Manager
 DOWN) = range(9)
N_STATES = 8 # (DOWN doesn't count as a state)
# Node states names
state_names = (
 'available',
 'busy',
 'off',
 'booting',
 'powoff',
 'failed',
 'failed-booting',
 'offline',
 'down',
 'idle')

# Field constants for events
(EV_TYPE) = (0)
# Event types
(NEW_JOB, NODE_INFO, EXCLUDE_NODE, USER_POWER, INIT_SESSION, REEVALUATE_JOB) = range(6)

# Field constants for requests
(RQ_ID,
 RQ_ANS,
 RQ_RM,
 RQ_NNODES,
 RQ_WHOLE_CLUSTER,
 RQ_TIME,
 RQ_NODE_SPEC,
 RQ_REEVAL_JOB_ID) = range(8)

# Constants for request state
(RQ_DONE, RQ_TIMEOUT, RQ_WAIT, RQ_ERROR) = range(4)

# Field constants for job_list
(JOB_RM_NAME, JOB_ID, JOB_STATE, JOB_NODES, JOB_WHOLE_CLUSTER, JOB_TIMESTAMP, JOB_SPECS, JOB_LAST_EVAL_TIME) = range(8)

# Requests Semaphore Object
req_lock = threading.Semaphore()

class CommandError(Exception):
   """Exception: a command returned a non-zero value"""
   pass

from exclude_nodes import *
from reports import *
from hooks import execute_hook

def get_node_pow_info(nodename, type):
   if len(POWER_INFO) == 1:
      group =  POWER_INFO.keys()[0]
      return POWER_INFO[group][type]
   elif len(POWER_INFO) > 1:
      for group in POWER_INFO.keys():
         if nodename in POWER_INFO[group]['nodes']:
            return POWER_INFO[group][type]
      
      return 0
   else:
      return 0

def run_command(command, shell=False):
   try:
      p = subprocess.Popen(command, stdout=subprocess.PIPE, 
                         stderr=subprocess.PIPE, shell=shell)
   except:
      if type(command)==list: command = string.join(command)
      logging.error('Could not execute command "%s"' %command)
      raise

   (out, err) = p.communicate()
   if p.returncode != 0:
      if type(command)==list: command = string.join(command)
      logging.error(' Error in command "%s"' % command)
      logging.error(' Return code was: %s' % p.returncode)
      logging.error(' Error output was:\n%s' % err)
      raise CommandError()
   else:
      return out

def cmd_switch_on(node):
   run_command([BIN_DIR+'/bootnode', node])

def cmd_switch_off(node):
   run_command([BIN_DIR+'/poweroff', node])

class ResourceManagerNodes:
   def __init__(self, rm_name):
      self.rm_name = rm_name
      now = time.time()
      self.node_pos = {}
      self.node_table = []
      self.state_counts = [0]*N_STATES
      info_nodes = self.cmd_node_info()
      self.n_nodes = len(info_nodes)
      self.job_list = []
      for (i,node_info) in enumerate(info_nodes):
         self.node_pos[node_info[NAME]] = i
         state = state_num[node_info[STATE]]
         if state==DOWN: state = OFF
         node = [node_info[NAME],state,now,
              True, # off_when_idle
              1,    # free_slots
              1,    # total_slots
              None, # dict
              None] # requirement evaluation
         read_node_info(node,node_info[2:])
         self.node_table.append(node)
         self.state_counts[state] += 1
      # Sort nodes by the power they consume
      self.set_power_sort()
      if not os.path.isfile(PLUGIN_DIR+'/'+self.rm_name+'/'+ self.rm_name+'.jobinfo'):
         logging.warning(self.rm_name+'.jobinfo script is not present. Job re-evaluation will be disabled.')

   def cmd_node_info(self):
      info_txt = run_command([PLUGIN_DIR+'/'+self.rm_name+'/'+
             self.rm_name+'.nodeinfo'])
      return [x.split(';') for x in info_txt.split('\n') if x]

   def cmd_job_info(self):
      if os.path.isfile(PLUGIN_DIR+'/'+self.rm_name+'/'+ self.rm_name+'.jobinfo'):
         info_txt = run_command([PLUGIN_DIR+'/'+self.rm_name+'/'+
                self.rm_name+'.jobinfo'])
         return [x.split(';') for x in info_txt.split('\n') if x]
      else:
         return []

   def n_nodes_on(self):
      n = self.state_counts[AVAILABLE]+ self.state_counts[BUSY]
      return n

   def n_nodes_fail(self):
      n = self.state_counts[FAIL]+ self.state_counts[FAIL_BOOTING]
      return n

   def set_power_sort(self):
      """Build index to have nodes sorted by the power they consume"""
      # Get power_used for each node
      aux = range(self.n_nodes)
      for (i,node_item) in enumerate(self.node_table):
         nodename = node_item[NAME]
         aux[i] = [i, get_node_pow_info(nodename, 'NODE_USED_POW')]
      # Sort nodes by its power_used
      aux.sort(key=lambda x: x[1])
      # Build index
      self.power_sort = [x[0] for x in aux]
      logging.debug("Nodes sorted by power: %s" %self.power_sort)


class Answer:
  # Un evento con un valor
  def __init__(self):
     self.ev = threading.Event()
     self.value = None
  def wait(self):
     self.ev.wait()
  def set(self,x):
     self.value = x
     self.ev.set()

class IdProvider:
   def __init__(self, max_id=10000):
      self.max_id = max_id
      self.next_id = 0
   def new_id(self):
      v = self.next_id
      self.next_id = (self.next_id+1)%self.max_id
      return v

class Server(SocketServer.ThreadingMixIn, SimpleXMLRPCServer): pass

# ---- Registered methods of the XMLRPC server

def new_job(rm_name, nnodes, whole_cluster, node_spec=None):
   """Switch on nodes for a new request, if necessary

      nnodes is the number of requested "virtual nodes" or vnodes. A "virtual
      node" is a group of slots in the same physical node.
      If whole_cluster is True, all nodes must be switched on, even if
      only nnodes nodes are requested
      If node_spec is not None, it is a string specifying nodes that are
      suitable for the request. It must have the form:
      '<var>=<value>;<var>=<value>;... ;keywords=<keyword>,<keyword>,...'""" 
   ans = Answer()
   events.put((NEW_JOB, ans, rm_name, nnodes, whole_cluster, node_spec))
   if not NEW_JOB_CALL_ASYNC:
      # If the function call is sync we must wait
      ans.wait()
      return ans.value
   else:
      return RQ_DONE

def exclude_node(token_session, node_name, exclude = True):
   """Excludes a node from the control of CLUES"""
   ans = Answer()
   events.put((EXCLUDE_NODE, ans, token_session, node_name, exclude))
   ans.wait()
   return ans.value

def user_power(token_session, node_name, power_on = True):
   """Powers on or off a node"""
   ans = Answer()
   events.put((USER_POWER, ans, token_session, node_name, power_on))
   ans.wait()
   return ans.value

def node_info(token_session):
   """Returns state of nodes for each LRMS"""
   ans = Answer()
   events.put((NODE_INFO, ans, token_session))
   ans.wait()
   return ans.value
   
def init_session(token_session):
   ans = Answer()
   events.put((INIT_SESSION, ans, token_session))
   ans.wait()
   return ans.value

# ---- Methods to process requests coming from registedred methods

def process_new_job(ans, rm_name, nnodes, whole_cluster, node_spec, reevaluated_job_id = None):
   """Process a new_job event"""
   global n_requested

   # Get manager info
   if managers.has_key(rm_name):
      rm = managers[rm_name]
   else:
      try:
         rm = ResourceManagerNodes(rm_name)
         managers[rm_name] = rm
      except (CommandError, OSError):
         logging.error('Error registering manager %s. Request cancelled' %rm_name)      
         ans.set(RQ_ERROR)
         return 
   
   if whole_cluster: msg = '(whole cluster)'
   else: msg = ''

   if reevaluated_job_id == None:
      logging.debug('Request from %s: %s vnodes %s (%s)' %(rm_name, nnodes, msg, node_spec))
      report_new_job(rm_name, nnodes)
   else:
      logging.debug('Reevaluating job %s from %s: %s vnodes %s (%s)' %(reevaluated_job_id, rm_name, nnodes, msg, node_spec))
      # check if this job has been already reevaluated
      job_evaluated = False
      logging.debug('Jobs previously evaluated:')
      req_lock.acquire()
      for req in requests:
         logging.debug(req[RQ_REEVAL_JOB_ID])
         if reevaluated_job_id == req[RQ_REEVAL_JOB_ID]:
            job_evaluated = True
      req_lock.release()
      # this job has been already reevaluated, nothing to do
      if job_evaluated:
         logging.debug('Job %s has been already reevaluated, skip this request.' % (reevaluated_job_id))
         return

   # Transformar node_spec a diccionario
   node_spec = read_node_spec(node_spec)

   req = [-1, ans, rm, nnodes, whole_cluster, time.time(), node_spec, reevaluated_job_id]
   req_stat = check_req(req, n_requested)
   if n_requested==0.0 and req_stat!=RQ_WAIT:
      release_req(req, req_stat)
   else:
      req_id = req_id_prov.new_id()
      req[0] = req_id
      req_lock.acquire()
      requests.append(req)
      req_lock.release()
      n_requested += nnodes*node_spec['slots']
      logging.debug('Request queued with id %s' %req_id)

def process_init_session(ans, token_session):
   if SECURITY_TOKEN_ENABLED:
      credential_server = AuthServer(AUTH_TOKEN_EXPIRE, CLUES_DB, True)
      success, token_session = credential_server.init_session_by_token(token_session)
      ans.set((success, token_session))
   else:
      ans.set((True, ""))

def process_node_info(ans, token_session):
   """Returns state of nodes for each LRMS"""
   if SECURITY_TOKEN_ENABLED:
      credential_server = AuthServer(AUTH_TOKEN_EXPIRE, CLUES_DB)
      success, token_session_obtained = credential_server.init_session_by_token(token_session)
      if not success:
         ans.set((False,[]))
         logging.warning("someone tried to accede using invalid credentials (%s)" % token_session)
         return

   r = []
   disabled_nodes = excluded_nodes()
   for rm in managers.values():
      nodes = []
      for item in rm.node_table:
         # FIX: this is an special case by which nodes are not considered
         # by CLUES. It would be interesting to consider a new state instead
         # maintaining a different list
         state = state_names[item[STATE]]
         if item[NAME] in disabled_nodes:
            state = 'disabled, ' + state
         nodes.append([item[NAME],state,item[FREE_SLOTS],item[TOTAL_SLOTS],item[TIME],item[DICT]])
      r.append((rm.rm_name, nodes))
      # ordenamos la lista por el nombre del nodo
      r = sorted(r,key=itemgetter(0)) 
   ans.set((True,r))
      
def process_req_info(ans):
   """Returns state of pending requests"""
   r = []
   req_lock.acquire()
   for (r_id, r_ans, rm, nnodes, whole_cluster, t, node_spec)  in requests:
      r.append([r_id, rm.rm_name, nnodes, whole_cluster, t, node_spec])
   req_lock.release()
   ans.set(r)

def process_exclude_node(ans, token_session, node_name, exclude):

   if SECURITY_TOKEN_ENABLED:
      credential_server = AuthServer(AUTH_TOKEN_EXPIRE, CLUES_DB)
      success, token_session_obtained = credential_server.init_session_by_token(token_session)
      if not success:
         ans.set((False, "not authorized"))

   if node_name == "_all_":
      nodename_list = get_node_list(managers)
   else:
      nodename_list = [node_name]

   global_result = True
   global_text = []

   for node_name in nodename_list:
      if exclude:
         if node_name not in initially_excluded_nodes():
            if disable_node(node_name):
               execute_hook('HOOK_DISABLED',[node_name])
               result = True
               result_text = "node %s sucessfully disabled" % node_name
            else:
               result = False
               result_text = "an error ocurred when disabling node %s. Please check logs" % node_name
         else:
            result = True
            result_text = "node %s already excluded" % node_name
      else:
         if node_name in initially_excluded_nodes():
            result = False
            result_text = "node %s cannot be enabled due to configuration issues" % node_name
         else:
            if enable_node(node_name):
               execute_hook('HOOK_ENABLED',[node_name])
               result = True
               result_text = "node %s sucessfully enabled" % node_name
            else:
               result = False
               result_text = "an error ocurred when enabling node %s. Please check logs" % node_name
               
      global_result = global_result and result
      global_text.append(result_text)

   ans.set((global_result, "\n".join(global_text)))

def process_user_power(ans, token_session, node_name, power_on):

   if SECURITY_TOKEN_ENABLED:
      credential_server = AuthServer(AUTH_TOKEN_EXPIRE, CLUES_DB)
      success, token_session_obtained = credential_server.init_session_by_token(token_session)
      if not success:
         ans.set((False, "not authorized"))

   if node_name == "_all_":
      nodename_list = get_node_list(managers)
   elif node_name == "_enabled_":
      all_nodes_list = get_node_list(managers)
      excluded_list = excluded_nodes()
      nodename_list = [ node for node in all_nodes_list if node not in excluded_list ]
   elif node_name == "_disabled_":
      all_nodes_list = get_node_list(managers)
      excluded_list = excluded_nodes()
      nodename_list = [ node for node in all_nodes_list if node in excluded_list ]
   else:
      nodename_list = [node_name]

   global_result = True
   global_text = []
   now = time.time()
   
   state_list = get_node_list_and_state(managers)
   for node_name in nodename_list:
      if node_name not in state_list:
         result = False
         result_text = "node %s is not under the control of clues" % node_name
      else:
         if power_on:
            # Hay que ver si esta en un estado en que se pueda encender
            # Puede ser APAGADO o FAIL
            if (state_list[node_name] == OFF or state_list[node_name] == FAIL):
               for rm in managers.values():
                  switch_on(rm, node_name)
               result = True
               result_text = "operation switch on invoked for node %s" % node_name

               # Esto deberia mejorarse, por ejemplo indexando el array rm.node_table por el nombre del nodo
               for (pos, node_item) in enumerate(rm.node_table):
                  if node_item[NAME]==node_name:
                     node_item[TIME] = now
                     break;
            else:
               result = False
               result_text = "node %s is not in a proper state (%d) to be powered on" % (node_name, state_list[node_name])
         else:
            # Hay que ver si esta en un estado en que se pueda apagar
            # unica posibilidad, de momento: ENCENDIDO
            if (state_list[node_name]==AVAILABLE):
               for rm in managers.values():
                  switch_off(rm, node_name)
               result = True
               result_text = "operation switch off invoked for node %s" % node_name
            else:
               result = False
               result_text = "node %s is not in a proper state (%d) to be powered off" % (node_name, state_list[node_name])
               
      global_result = global_result and result
      global_text.append(result_text)

   ans.set((global_result, "\n".join(global_text)))
               
# ------------

def read_node_spec(spec_text):
   """Transformar spec_list a diccionario"""
   # Tenemos que quitar los parametros fijos o que sean listas: queues, keywords, hostgroups, slots y hostname
   eval_spec_text = ""
   node_spec = {'slots':1}
   if spec_text is not None:
      for x in spec_text.split(';'):
         pair = x.split('=')
         if len(pair) == 2:
            # it tries to detect the keywords
            if pair[0] in ['slots']:
               node_spec[pair[0]] = float(pair[1])
            # Realmente al hostname no le hace falta hacer el split, pero hace mas facil las comparaciones despues
            elif pair[0] in ['queues', 'hostgroups', 'keywords','hostname']:
               node_spec[pair[0]] = pair[1].split(',')
            else:
               eval_spec_text += x
         else:
            eval_spec_text += x

   node_spec['_'] = eval_spec_text
   
   return node_spec

def read_node_info(node, info):
   """Reads node information (info) into var node"""
   node[OFF_WHEN_IDLE]=True
   d = {}
   for x in info:
      try:
         pair = x.split('=')
         if pair[0]=='keywords':
            d[pair[0]] = pair[1].split(',')
         elif pair[0]=='free_slots':
            node[FREE_SLOTS] = float(pair[1])
         elif pair[0]=='total_slots':
            node[TOTAL_SLOTS] = float(pair[1])
         elif pair[0]=='off_when_idle':
            if pair[1].lower()=='y': node[OFF_WHEN_IDLE]=True
            elif pair[1].lower()=='n': node[OFF_WHEN_IDLE]=False
            else: raise ValueError()
         else:
            d[pair[0]] = pair[1]
      except (ValueError, IndexError):
         logging.error('Error reading info about node %s (%s)'
                       %(node[NAME],x))
   node[DICT] = d
   req = evaluation.ReqsEvaluation()
   for k, v in d.iteritems():
      req.add_var_detect_numeric(k, v)
      
   node[REQOBJ] = req

def jobs_update():
   """ Update state of jobs"""
   # Update node states for each rm
   for rm in managers.values():
      update_jobs(rm)

def nodes_update():
   """ Update state of nodes and reqs"""
   # Update node states for each rm
   for rm in managers.values():
      update_nodes(rm)

   # Now the node states are updated
   # check for reqs to be released
   check_all_reqs()

   # Swicth off idle nodes
   for rm in managers.values():
      check_idle(rm)

   # print state info to log
   log_states()

def read_job_info(job_info):
   job = [job_info[JOB_RM_NAME], job_info[JOB_ID], job_info[JOB_STATE], job_info[JOB_NODES], job_info[JOB_WHOLE_CLUSTER], job_info[JOB_TIMESTAMP], None, None]
   job[JOB_SPECS] = ";".join(job_info[6:])
   
   return job

def update_jobs(rm):
    try:
       info_jobs = rm.cmd_job_info()
    except (CommandError, OSError):
       logging.error('Could not update job info')
    else:
       # check if there are new jobs
       for job_info in info_jobs:
          job = read_job_info(job_info)
          is_new_job = True
          for old_job in rm.job_list:
             if job[JOB_ID] == old_job[JOB_ID]:
               is_new_job = False

          if is_new_job:
             logging.debug("New Job in the LRMS: " + job[JOB_ID])
             # add them to the list
             job[JOB_NODES] = int(job[JOB_NODES])
             if job[JOB_WHOLE_CLUSTER] == 'True':
               job[JOB_WHOLE_CLUSTER] = True
             else:
               job[JOB_WHOLE_CLUSTER] = False
             if job[JOB_SPECS] == '':
                job[JOB_SPECS] = None
             job[JOB_TIMESTAMP] = int(job[JOB_TIMESTAMP])
             # add the JOB_LAST_EVAL_TIME
             #job.append(job[JOB_TIMESTAMP])
             # Lo cambio para evitar reevaluaciones al rearrancar clues
             # y para evitar reevaluaciones en caso de problemas al reaparecer trabajos
             job[JOB_LAST_EVAL_TIME] = int(time.time())
             report_new_lrms_job(job)
             rm.job_list.append(job)

       # check if some job has finished
       finished_jobs = []
       for old_job in rm.job_list:
          isalive = False
          for job in info_jobs:
             if job[JOB_ID] == old_job[JOB_ID]:
                old_job[JOB_STATE] = job[JOB_STATE]
                old_job[JOB_TIMESTAMP] = job[JOB_TIMESTAMP]
                isalive = True

          if not isalive:
             logging.debug("Job " + old_job[JOB_ID] + " finished.")
             # the job has finished
             # TODO: save the length of the job
             report_end_lrms_job(old_job)
             finished_jobs.append(old_job)

       # Remove all the finished jobs from the list
       for job in finished_jobs:
             rm.job_list.remove(job)

       # check if some job must be evaluated
       if TIME_TO_EVALUATE > 0:
         for job in rm.job_list:
            now = int(time.time())
            # if the job is queued for many time
            if job[JOB_STATE] == 'Q' and (now - job[JOB_LAST_EVAL_TIME]) >  TIME_TO_EVALUATE:
               # evaluate the job
               logging.info("Evaluating queued job: " + job[JOB_ID])
               job[JOB_LAST_EVAL_TIME] = now
               ans = Answer()
               events.put((REEVALUATE_JOB, ans, job[JOB_RM_NAME], job[JOB_NODES], job[JOB_WHOLE_CLUSTER], job[JOB_SPECS], job[JOB_ID]))

def update_nodes(rm):
    """Get node states from LRMS and update state info accordingly"""
    # Podria haber una funcion mas ligera, que solo obtuviera los
    # nodos disponibles (no los apagados) y actualizara los estados (el
    # bucle de actualizacion seria identico).
    # Seria la funcion a llamar para actualizar despues de lanzar trabajo

    execute_hook('HOOK_MONITORING',[rm.rm_name])

    try:
       info_nodes = rm.cmd_node_info()
    except (CommandError, OSError):
       logging.error('Could not update node state info')
    else: 
       rm.state_counts = [0]*N_STATES
       for x in info_nodes:
          if x[0] in rm.node_pos:
             pos = rm.node_pos[x[0]]
             node_item = rm.node_table[pos]
             old_state = node_item[STATE]
             change_node_state(node_item, state_num[x[1]])
             old_free_slots = node_item[FREE_SLOTS]
             read_node_info(node_item, x[2:])
             if node_item[STATE] != old_state or node_item[FREE_SLOTS] != old_free_slots:
                # the state or the number of free slots of the node has changed
                # save the info for this node
                report_node(node_item[NAME], node_item[STATE], node_item[FREE_SLOTS], node_item[TOTAL_SLOTS])

             rm.state_counts[node_item[STATE]] += 1
          else:
             logging.warning('Ignoring unknown node %s. If you want to take it into account, please restart clues.' % x[0])
    
    # Check if there are not enough nodes available
    if (rm.rm_name in NODES_AVAILABLE and rm.state_counts[OFF]>0 and
        (rm.state_counts[AVAILABLE]+rm.state_counts[BOOTING])<NODES_AVAILABLE[rm.rm_name]):
       # we must switch on more
       n = NODES_AVAILABLE[rm.rm_name] - (rm.state_counts[AVAILABLE]+rm.state_counts[BOOTING])
       logging.debug("Switching on %d nodes to maintain %d nodes available" % (n, NODES_AVAILABLE[rm.rm_name]))
       switch_on_nodes(rm, n)
      
    execute_hook('HOOK_MONITORED',[rm.rm_name])

def change_node_state(node_item, new_state):
       node = node_item[NAME]
       now = time.time()
       if node_item[STATE] in (BOOTING, FAIL_BOOTING):
          if new_state==DOWN:
             # Booting node is still down. Check if timeout exceeded
             if now-node_item[TIME] > MAX_TIME_BOOT:
                logging.error('Node %s could not be switched on (timeout)'
                       % node)
                new_state = FAIL
                # Call the hook only the first time the node fails
                if node_item[STATE] == BOOTING:
                   execute_hook('HOOK_FAIL',[node])
             else:
                new_state = node_item[STATE] 
          elif new_state==OFFLINE:
             logging.info('Node %s is offline' %node)
          else:
             # Booting node is already on
             logging.info('Node %s is on' %node)
             # Mark node as busy, to avoid switching it off
             node_item[TIME] = now
             execute_hook('HOOK_POWEREDON',[node])
       elif node_item[STATE]==POWOFF:
          if now-node_item[TIME] > MAX_TIME_POWOFF:
             if new_state == DOWN:
                logging.info('Node %s is now off' %node)
                new_state = OFF
                execute_hook('HOOK_POWEREDOFF',[node])
             elif new_state==OFFLINE:
                logging.info('Node %s is offline' %node)
             else:
                logging.error('Node %s could not be switched off' %node)
          else:
             new_state = POWOFF
       elif node_item[STATE] in (AVAILABLE, BUSY):
          if new_state==DOWN:
             logging.warning('Node %s was switched off unexpectedly' %node)
             new_state = OFF
             execute_hook('HOOK_POWEREDOFF_UNEXPECTED',[node])
          elif new_state==OFFLINE:
             logging.info('Node %s is offline' %node)
       elif node_item[STATE] in (OFF,FAIL):
          if new_state==DOWN:
             new_state = node_item[STATE]
          elif new_state==OFFLINE:
             logging.info('Node %s is offline' %node)
          else:
             logging.warning('Node %s was switched on unexpectedly' %node)
             execute_hook('HOOK_POWEREDON_UNEXPECTED',[node])
             # We are going to avoid starting the switching off counter when the node
             # is switched on unexpectedly. The reason: if the administrator wants to
             # deal with the node outside the control of CLUES, he should exclude from
             # the system.
             # By simply using this approach, we will achieve at the next to states
             # a) the node is supposed to be off and it is switched on unexpectedly:
             #    * the node will be switched off as it is supossed to be in such state
             # b) the node is supposed to be on and it is switched on unexpectedly:
             #    * the node will remain in the switched on state as it is the state in
             #      which it should be, but it will be in the same situation as before
             #      the unexpectedly issues: it will be switched off when the grace time
             #      is reached.
             # node_item[TIME] = now 
       elif node_item[STATE]==OFFLINE:
          if new_state!=OFFLINE:
             if new_state==DOWN: new_state=OFF
             logging.info('Node %s is online and %s' %(node,state_names[new_state]))

       node_item[STATE] = new_state


def check_all_reqs():
   """Check for requests to be released and release them"""

   # Step 1: release requests from the beginning of the queue 
   released=0
   n_reqs=0
   req_lock.acquire()
   for req in requests:
      req_stat = check_req(req, n_reqs)
      if req_stat == RQ_WAIT:
         # This request and the folowing are kept in queue.
         n_reqs = req[RQ_NNODES]*req[RQ_NODE_SPEC]['slots']
         break
      else:
         release_req(req, req_stat)
         released += 1
   del requests[0:released] 
   
   # Step 2: go through remaining requests and switch on nodes if necessary
   for req in requests[1:]:
      req_stat = check_req(req, n_reqs)
      n_reqs += req[RQ_NNODES]*req[RQ_NODE_SPEC]['slots']
   req_lock.release()


def check_req(req, n_reqs):
   (req_id, ans, rm, nnodes, whole_cluster, t, node_spec, reeval_job_id) = req

   if req_id==-1: req_text="Request"
   else: req_text="Request %s" % req_id

   if rm.n_nodes_on()+rm.n_nodes_fail()+rm.state_counts[OFFLINE]==rm.n_nodes:
      # All nodes are on/offline/failed
      # All we can do is to try to switch on failed nodes
      logging.debug("All nodes are on/offline/failed. Try to switch on failed nodes.")
      if rm.n_nodes_fail()>0 and req_id==-1:
         switch_on_nodes(rm)
      req_stat=RQ_DONE

   elif whole_cluster:
      # Just switch everything on
      logging.debug("Whole Cluster flag active: switch everything on")
      switch_on_nodes(rm)
      if rm.state_counts[BOOTING]+rm.state_counts[POWOFF]==0:
         # Everything was already on
         req_stat=RQ_DONE
      else:
         req_stat=RQ_WAIT

   else:
      (state_ct, off_l) = eval_spec(rm, node_spec, n_reqs)
      # Check if there are enough nodes. If not, switch on nodes
      if (state_ct[AVAILABLE]>=nnodes or
         state_ct[OFF]+state_ct[BOOTING]+state_ct[POWOFF]==0):
         # Request satisfied or impossible to satisfy.
         logging.debug("%s satisfied or impossible to satisfy." %req_text)
         logging.debug("There are %d available and %d booting for this request."
                       % (state_ct[AVAILABLE], state_ct[BOOTING]))
         req_stat=RQ_DONE
      else:
         # Switch on nodes if necessary
         logging.debug("%s asks for %d vnodes. There are %d available and %d booting for this request." 
                    % (req_text, nnodes, state_ct[AVAILABLE], state_ct[BOOTING]))
         n = nnodes-state_ct[AVAILABLE]-state_ct[BOOTING]
         
         if n>0 and state_ct[OFF]>0:
            switch_on_nodes(rm, n, off_l)
         req_stat=RQ_WAIT

   if req_stat==RQ_WAIT:
      # Check if timeout has been reached
      if time.time()-t > MAX_WAIT_JOB:
         req_stat=RQ_TIMEOUT
      else:
         logging.debug("Request must wait.")

   return req_stat

def check_idle(rm):
   """Check for idle nodes and switch them off"""
   for node_item in rm.node_table:
      node = node_item[NAME]
      # If the node is not excluded and it can be switched off and there is no request waiting
      if node not in excluded_nodes() and node_item[OFF_WHEN_IDLE] and n_requested==0.0:
         # If the node is available and all the slots are free
         if (node_item[STATE]==AVAILABLE and 
             abs(node_item[FREE_SLOTS]-node_item[TOTAL_SLOTS])<0.01):
            # node has been idle for a long time
            if time.time()-node_item[TIME] > MAX_TIME_IDLE:
               # There are enough available nodes 
               if not rm.rm_name in NODES_AVAILABLE or rm.state_counts[AVAILABLE] > NODES_AVAILABLE[rm.rm_name]:
                  switch_off(rm, node)

      if (node_item[STATE] == BUSY or
         (node_item[STATE] == AVAILABLE and 
#          node_item[FREE_SLOTS] < node_item[TOTAL_SLOTS])):
          (node_item[TOTAL_SLOTS]-node_item[FREE_SLOTS])>0.01)):
            # node is being used, update time last used
            node_item[TIME] = time.time()

def eval_spec(rm, node_spec, n_req):
   """Evaluate spec for nodes

      Returns (vnode_cnt, off_l), where:
      vnode_cnt is the number of matching vnodes in each state
      off_l is a list of pairs in the form [pos, n] where pos is 
      the position of a node that is not on and provides n matching 
      vnodes"""
   n_av = 0
   n_boot = 0
   vnode_cnt=[0]*N_STATES
   off_l=[]
   for i in rm.power_sort:
      node = rm.node_table[i]
      n_vnodes = spec_matching(node_spec, node)
      state=node[STATE]
      if n_vnodes>0:
         vnode_cnt[state] += n_vnodes
         if state in (OFF, FAIL, POWOFF): off_l.append([i,n_vnodes])
      if state==AVAILABLE: n_av += node[FREE_SLOTS]
      if state==BOOTING: n_boot += node[TOTAL_SLOTS]

   # Take into account n_req (previously requested slots) and estimate number
   # of available and booting vnodes left for this request.
   # Note: we make best-case estimates
   slots=node_spec['slots']
   vnode_cnt[AVAILABLE] = min(max(0, int((n_av-n_req+0.01)/slots)), 
                              vnode_cnt[AVAILABLE])
   x = int((n_av+n_boot-n_req+0.01)/slots)-vnode_cnt[AVAILABLE]
   vnode_cnt[BOOTING] = min(max(0, x), vnode_cnt[BOOTING])
   return (vnode_cnt, off_l)


def spec_matching(node_spec, node):
   """Checks if a given node_spec matches a node. In case of match, the number
      of vnodes provided by the node is returned. Otherwise, zero is returned"""

   logging.debug("evaluating specs %s for node %s" % (node_spec, node[NAME]))
   if (node[STATE] in (BUSY,OFFLINE,FAIL_BOOTING) or 
       (node[STATE] in (OFF,POWOFF) and node[NAME] in excluded_nodes())): 
      # The node is not usable (number of provided vnodes is zero)
      return 0

   if node[STATE]==AVAILABLE: free_slots=node[FREE_SLOTS]
   else: free_slots=node[TOTAL_SLOTS]
   n_vnodes = free_slots
   match=True
   for key, value in node_spec.iteritems():
      if key=='_':
         pass
      elif key=='slots':
         n_vnodes = int((free_slots+0.01)/value)
      # the node must fulfil all the keywords
      elif key in ['keywords']:
         if node[DICT][key] is None:
            match=False
         else:
            for kword in value:
               if kword not in node[DICT][key]:
                  match=False
                  break
      # but the node must only have at least one of these elems
      elif key in ['queues','hostgroups','hostname']:
         if not key in node[DICT] or node[DICT][key] is None:
            match=False
         else:
            match=False
            for elem in value:
               if elem in node[DICT][key]:
                  match=True
            if not match:
               break;
            
      #elif value!=node[DICT][key]:
      #   match=False

   if match:
      try:
         if node_spec['_'] != "":
            match = node[REQOBJ].eval(node_spec['_'])
      except:
         match = False

   if not match:
      n_vnodes=0
   return n_vnodes


def switch_on_nodes(rm, n_vnodes=None, node_l=None):
   """Switch on a number of nodes of a resource manager

      n_vnodes is the number of vnodes to be switched on
      node_l is a list of pairs in the form [pos, n] where pos is 
      the position of a node that provides n matching vnodes"""
   if node_l==None:
      node_l=[[i,1] for i in rm.power_sort]
      if n_vnodes==None: n_vnodes=rm.n_nodes
      else: n_vnodes+=extra_nodes(n_vnodes)
      switch_on_nodes_simple(rm, n_vnodes, node_l)
   else:
      # switch on num. of vnodes requested
      (n_on, inext)=switch_on_nodes_simple(rm, n_vnodes, node_l)
      # switch on extra nodes
      n_extra=extra_nodes(n_on)
      if n_extra>0:
         # Nodes suitable for this request are considered first, then
         # all nodes
         node_l=([[i,1] for i,n in node_l[inext:]]+
                 [[i,1] for i in rm.power_sort])
         (n_on,inext)=switch_on_nodes_simple(rm, n_extra, node_l)


def switch_on_nodes_simple(rm, n_vnodes, node_l):
   n_vn_on = 0
   n_on = 0
   i=-1
   for i,(pos,n) in enumerate(node_l):
      node_item = rm.node_table[pos]
      if (node_item[NAME] not in excluded_nodes() and 
         node_item[STATE] in (OFF,FAIL)):
         switch_on(rm,node_item[NAME])
         if node_item[STATE]==BOOTING:
            n_vn_on+=n
            n_on+=1
            if n_vn_on>=n_vnodes: break
   return (n_on, i+1)


def extra_nodes(n_on):
   """Compute number of extra nodes"""
   blocks = (n_on+SWITCH_ON_BLOCKSIZE-1)/SWITCH_ON_BLOCKSIZE
   n_extra= blocks*SWITCH_ON_BLOCKSIZE-n_on
   return n_extra
      
       
def switch_on(rm, node):
   """Switch on a particular node of a resource manager"""
   logging.info('Switching on node %s' %node)
   try:
      execute_hook('HOOK_POWERON',[node])
      cmd_switch_on(node)
#      logging.info('Node %s successfully switched on' %node)
   except (CommandError, OSError):
      pass
   else:
      node_item = rm.node_table[rm.node_pos[node]]
      rm.state_counts[node_item[STATE]] -= 1
      if node_item[STATE]==FAIL:
         node_item[STATE] = FAIL_BOOTING
      else:
         node_item[STATE] = BOOTING
      rm.state_counts[node_item[STATE]] += 1
      node_item[TIME] = time.time()


def switch_off(rm, node):
   """Switch off a particular node of a resource manager"""
   logging.info('Switching off node %s' %node)
   try:
      execute_hook('HOOK_POWEROFF',[node])
      cmd_switch_off(node)
   except (CommandError, OSError):
      pass
   else:
      node_item = rm.node_table[rm.node_pos[node]]
      rm.state_counts[node_item[STATE]] -= 1
      rm.state_counts[POWOFF] += 1
      node_item[STATE] = POWOFF
      node_item[TIME] = time.time()

def release_req(req, req_stat):
   global n_requested
   (req_id, ans, rm, nnodes, whole_cluster, t, node_spec, reeval_job_id) = req
   ans.set(req_stat)
   if req_stat==RQ_TIMEOUT:
      msg=' (timeout)'
   else:
      msg=''
   if req_id>=0:
      n_requested -= nnodes*node_spec['slots']
      if abs(n_requested)<0.01: n_requested=0.0
      logging.debug('Request %s processed%s' %(req_id, msg))
   else:
      logging.debug('Request processed%s' % msg)
   if reeval_job_id == None:
      report_release_job(rm.rm_name, nnodes)

   # if the job need the whole cluster, update the time in all of them
   if whole_cluster:
      for node_item in rm.node_table:
         node_item[TIME] = time.time()

   req_released(rm)
    

def req_released(rm):
   """Wait for the req to be launched by LRMS, and update info about nodes"""
   time.sleep(MAX_TIME_LAUNCH)
   update_nodes(rm)

def log_states():
   """Print to log number of nodes in each state"""
   for rm in managers.values():
      msg = ('%s nodes: Avail:%s Busy:%s Off:%s Boot:%s Powoff:%s'
                  %(rm.rm_name, 
                    rm.state_counts[AVAILABLE],
                    rm.state_counts[BUSY],
                    rm.state_counts[OFF],
                    rm.state_counts[BOOTING],
                    rm.state_counts[POWOFF]))
      n=0
      for node_item in rm.node_table:
          if node_item[OFF_WHEN_IDLE]: n+=1
      msg += ' Off_when_idle:%s' %n

      if rm.state_counts[FAIL]>0:
         msg += ' Fail:%s' %rm.state_counts[FAIL]
      if rm.state_counts[FAIL_BOOTING]>0:
         msg += ' FailBoot:%s' %rm.state_counts[FAIL_BOOTING]
      if rm.state_counts[OFFLINE]>0:
         msg += ' Offline:%s' %rm.state_counts[OFFLINE]
      msg += ' Req:%.1f' %n_requested
      logging.debug(msg)

def serve_forever():
   server = Server((SERVER_HOST, SERVER_PORT))
   server.register_function(new_job)
   server.register_function(node_info)
   server.register_function(exclude_node)
   server.register_function(user_power)
   server.register_function(init_session)
   server.serve_forever()

def init():
   global events, requests, n_requested, managers, req_id_prov, state_num

   logging.basicConfig(filename=LOGFILE, 
            level=logging.DEBUG, 
            format='%(asctime)s: %(levelname)-8s %(message)s',
            datefmt='%m-%d-%Y %H:%M:%S')
   logging.info('************ Starting clues_daemon ' + CLUES_VERSION_TAG + ' ************')

   report_init()

   state_num={'free': AVAILABLE, 'busy': BUSY, 'down': DOWN, 'offline':OFFLINE}
   events = Queue.Queue()
   requests=[]
   n_requested=0.0
   managers={}
   req_id_prov = IdProvider()
   for rm_name in MANAGERS:
      try:
         logging.info('Registering resource manager %s' % rm_name)
         managers[rm_name]=ResourceManagerNodes(rm_name)
      except (CommandError, OSError):
         logging.error('Error registering manager %s' %rm_name)
   # save the first state
   report_nodes(managers)
   read_excluded_list(EXCLUDED_NODES_FILE)


if __name__=="__main__":

   NOTICE="\n\n\
CLUES - Cluster Energy Saving System\n\
Copyright (C) 2011 - GRyCAP - Universitat Politecnica de Valencia\n\
This program comes with ABSOLUTELY NO WARRANTY; for details please\n\
read the terms at http://www.gnu.org/licenses/gpl-3.0.txt.\n\
This is free software, and you are welcome to redistribute it\n\
under certain conditions; please read the license at \n\
http://www.gnu.org/licenses/gpl-3.0.txt for details."

   # Parse options
   parser = OptionParser(usage="%prog [--max-idle <max idle time>]"+NOTICE, version="%prog "+CLUES_VERSION_TAG)
   parser.add_option("--max-idle", action="store", type="int",
                     dest="MAX_TIME_IDLE",
                     help="Idle time for a node to be switched off")
   
   (options, args) = parser.parse_args()
   if options.MAX_TIME_IDLE != None:
      MAX_TIME_IDLE=options.MAX_TIME_IDLE
   
   # Basic initialization
   init()

   # Start server thread
   thr_server = threading.Thread(target=serve_forever)
   thr_server.setDaemon(True)
   thr_server.start()
   
   # Create thread variables for the update processes
   thr_nodes = None
   thr_jobs = None
   
   # main loop
   t_update = time.time()
   while True:
      t_wait = t_update-time.time()
      update = (t_wait<=0)
      if not update:
         try:
            ev = events.get(True, t_wait)
            if ev[EV_TYPE]==NEW_JOB:
               process_new_job(*ev[1:])
            elif ev[EV_TYPE]==REEVALUATE_JOB:
               process_new_job(*ev[1:])
            elif ev[EV_TYPE]==NODE_INFO:
               process_node_info(*ev[1:])
            elif ev[EV_TYPE]==EXCLUDE_NODE:
               process_exclude_node(*ev[1:])
            elif ev[EV_TYPE]==USER_POWER:
               process_user_power(*ev[1:])
            elif ev[EV_TYPE]==INIT_SESSION:
               process_init_session(*ev[1:])

         except Queue.Empty:
            update=True
      if update:
         t_update += TIME_UPDATE_STATUS

         # Launch the update processes in threads
         # If the previous thread is still alive, do not launch it again
         if thr_nodes is None or not thr_nodes.isAlive():
            thr_nodes = threading.Thread(target=nodes_update)
            thr_nodes.start()
            
         if thr_jobs is None or not thr_jobs.isAlive():
            thr_jobs = threading.Thread(target=jobs_update)
            thr_jobs.start()

   
