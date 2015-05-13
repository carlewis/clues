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

import time
import logging
from clues_daemon import AVAILABLE, BUSY, OFF, BOOTING, POWOFF, FAIL, FAIL_BOOTING, DOWN, logging, CLUES_DB, NAME, STATE, FREE_SLOTS, TOTAL_SLOTS, JOB_RM_NAME, JOB_ID, JOB_STATE, JOB_NODES, JOB_WHOLE_CLUSTER, JOB_SPECS, JOB_TIMESTAMP, JOB_LAST_EVAL_TIME
from db import DataBase

def report_nodes(managers):
	for rm in managers.values():
		for (pos, node_item) in enumerate(rm.node_table):
			report_node(node_item[NAME], node_item[STATE], node_item[FREE_SLOTS], node_item[TOTAL_SLOTS])

def report_node(name, state, free_slots, total_slots):
	now = float(time.time())
	db = DataBase(CLUES_DB)
	if DataBase.db_available:
		db.connect()
		t = (now, name, state, free_slots, total_slots)
		db.execute('insert into report_nodes values (?,?,?,?,?)', t)
		db.close()
	else:
		msg = ('REPORT NODE %s;%s;%s;%s' % (name, state, free_slots, total_slots))
		logging.info(msg)

def report_job_reset():
	now = float(time.time())
	db = DataBase(CLUES_DB)
	if DataBase.db_available:
		db.connect()
		t = (now, '', -1, -1)
		db.execute('insert into report_jobs values (?,?,?,?)', t)
		db.close()
	else:
		msg = ('REPORT JOB ;-1;-1')
		logging.info(msg)

def report_new_job(manager, nnodes):
	now = float(time.time())
	db = DataBase(CLUES_DB)
	if DataBase.db_available:
		db.connect()
		t = (now, manager, nnodes, 0)
		db.execute('insert into report_jobs values (?,?,?,?)', t)
		db.close()
	else:
		msg = ('REPORT JOB %s;%s;0' % (manager, nnodes))
		logging.info(msg)

def report_release_job(manager, nnodes):
	now = float(time.time())
	db = DataBase(CLUES_DB)
	if DataBase.db_available:
		db.connect()
		t = (now, manager, 0, nnodes)
		db.execute('insert into report_jobs values (?,?,?,?)', t)
		db.close()
	else:
		msg = ('REPORT JOB %s;0;%s' % (manager, nnodes))
		logging.info(msg)

def report_new_lrms_job(job):
	db = DataBase(CLUES_DB)
	if DataBase.db_available:
		db.connect()
		result = db.select("select JobID from lrms_jobs where JobID = '" + job[JOB_ID] + "'")
		logging.debug("The jobs with JobID " + job[JOB_ID] + " is yet in the DB, skip it.")
		if (result is None or len(result) == 0):
			if job[JOB_WHOLE_CLUSTER]:
				t = (job[JOB_RM_NAME], job[JOB_ID], job[JOB_NODES], job[JOB_SPECS], 1, job[JOB_TIMESTAMP])
			else:
				t = (job[JOB_RM_NAME], job[JOB_ID], job[JOB_NODES], job[JOB_SPECS], 0, job[JOB_TIMESTAMP])
			db.execute('insert into lrms_jobs values (?,?,?,?,?,?,0,0)', t)

		db.close()
	else:
		msg = ('REPORT NEW_LRMS_JOB %s;%s;%s;%s;%s;%s;0;0'
		      %(job[JOB_RM_NAME], job[JOB_ID], job[JOB_NODES], job[JOB_SPECS], job[JOB_WHOLE_CLUSTER], job[JOB_TIMESTAMP]))
		logging.info(msg)
	

def report_end_lrms_job(job):
	now = float(time.time())
	db = DataBase(CLUES_DB)
	if DataBase.db_available:
		db.connect()
		db.execute("update lrms_jobs set StartTime = " + str(job[JOB_TIMESTAMP]) + ", EndTime = " + str(now)
			   + " where JobID = '" + job[JOB_ID] + "' and Manager = '" + job[JOB_RM_NAME] + "'")
		db.close()
	else:
		msg = ('REPORT END_LRMS_JOB %s;%s;%s;%s'
		      %(job[JOB_RM_NAME], job[JOB_ID], job[JOB_TIMESTAMP], now))
		logging.info(msg)


def report_init(dbfile = CLUES_DB):
	db = DataBase(dbfile)
	if DataBase.db_available:
		db.connect()
		if not db.table_exists("report_jobs"):
			# la tabla no existe y la vamos a crear
			sentence = '''create table "report_jobs" ('''
			sentence = sentence + ''' "date" FLOAT NOT NULL, '''
			sentence = sentence + ''' "Manager" VARCHAR(128) NOT NULL, '''
			sentence = sentence + ''' "NewJob" INTEGER NOT NULL, '''
			sentence = sentence + ''' "ReleasedJob" INTEGER NOT NULL '''
			sentence = sentence + ''' ) '''
			db.execute(sentence)
		if not db.table_exists("report_nodes"):
			# la tabla no existe y la vamos a crear
			sentence = '''create table "report_nodes" ('''
			sentence = sentence + ''' "date" FLOAT NOT NULL, '''
			sentence = sentence + ''' "Node" VARCHAR(256) NOT NULL, '''
			sentence = sentence + ''' "State" INTEGER NOT NULL, '''
			sentence = sentence + ''' "FreeSlots" REAL, '''
			sentence = sentence + ''' "TotalSlots" REAL'''
			sentence = sentence + ''' ) '''
			db.execute(sentence)
		if not db.table_exists("lrms_jobs"):
			# la tabla no existe y la vamos a crear
			sentence = '''create table "lrms_jobs" ('''
			sentence = sentence + ''' "Manager" VARCHAR(128) NOT NULL, '''
			sentence = sentence + ''' "JobID" INTEGER NOT NULL, '''
			sentence = sentence + ''' "Nodes" INTEGER NOT NULL, '''
			sentence = sentence + ''' "Specs" VARCHAR(256), '''
			sentence = sentence + ''' "WholeCluster" INTEGER NOT NULL, '''
			sentence = sentence + ''' "QueuedTime" FLOAT NOT NULL, '''
			sentence = sentence + ''' "StartTime" FLOAT, '''
			sentence = sentence + ''' "EndTime" FLOAT '''
			sentence = sentence + ''' ) '''
			db.execute(sentence)
		
		db.close()

	report_job_reset()
