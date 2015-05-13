#! /usr/bin/env python
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

from clues_daemon import AVAILABLE, BUSY, OFF, BOOTING, POWOFF, FAIL, FAIL_BOOTING, OFFLINE, DOWN
from clues_daemon import get_node_pow_info
from reports import report_init
from optparse import OptionParser
from datetime import datetime, timedelta
from db import DataBase
import time
import sys
import os
import glob

NOTICE="\n\n\
CLUES - Cluster Energy Saving System\n\
Copyright (C) 2011 - GRyCAP - Universitat Politecnica de Valencia\n\
This program comes with ABSOLUTELY NO WARRANTY; for details please\n\
read the terms at http://www.gnu.org/licenses/gpl-3.0.txt.\n\
This is free software, and you are welcome to redistribute it\n\
under certain conditions; please read the license at \n\
http://www.gnu.org/licenses/gpl-3.0.txt for details."

from config import *
try:
   from config_local import *
except:
   pass

if not DataBase.db_available:
	print "SQLite not available. Install SQLite python support to use this command."
	sys.exit(1)

# Data fields
DATE = 0
MANAGER = 1
NUM_JOBS_DATA_FIELDS = 4
NEW_JOB = 2
RELEASE_JOB = 3
NUM_NODES_DATA_FIELDS = 5
NODE_NAME = 1
STATUS = 2
FREE_SLOTS = 3
TOTAL_SLOTS = 4

# LRMS jobs fields
NUM_LRMS_JOBS_DATA_FIELDS = 8
(JOB_RM_NAME, JOB_ID, JOB_NODES, JOB_SPECS, JOB_WHOLE_CLUSTER, JOB_QUEUE_TIME, JOB_START_TIME, JOB_END_TIME) = range(NUM_LRMS_JOBS_DATA_FIELDS)
END_JOB_START_TIME = 2
END_JOB_END_TIME = 3


def load_data_from_logfiles(init_date=None):
	fileNames = glob.glob(LOGFILE + "*")
	nodes = []
	jobs = []
	lrms_jobs = []
	for f in fileNames:
		if f.endswith(".gz"):
			os.system("gunzip " + f)
			f = f[0:len(f)-3]
			(sub_jobs, sub_nodes, sub_lrms_jobs) = load_log_file(f, init_date)
			nodes.extend(sub_nodes)
			jobs.extend(sub_jobs)
			lrms_jobs.extend(sub_lrms_jobs)
			os.system("gzip " + f)
		else:
			(sub_jobs, sub_nodes, sub_lrms_jobs) = load_log_file(f, init_date)
			nodes.extend(sub_nodes)
			jobs.extend(sub_jobs)
			lrms_jobs.extend(sub_lrms_jobs)

	return (jobs, nodes, lrms_jobs)

def load_log_file(logfile, init_date):
        f = open(logfile)
        try:
                # para almacenar los datos
                nodes = []
                jobs = []
                lrms_jobs = []
                for line in f:
                        # filtramos el fichero por la palabra REPORT
                        if line.find('REPORT') != -1 :
                                # primero quitamos la informacion innecesaria
                                row = []
                                words = line.split(' ')
                                # y sacamos la fecha
                                str_date = '%s %s' % (words[0], words[1][0:8])
                                date = datetime.fromtimestamp(time.mktime(time.strptime(str_date, '%m-%d-%Y %H:%M:%S')))
                                type = words[8]
                                # ahora leemos los datos
                                data = words[9]
                                fields = data.split(';')

                                if init_date == None or date > init_date:
                                        # los anyadimos si son posteriores a la fecha de inicio, o no se ha puesto fecha
                                        row.append(date)
                                        for val in fields:
                                                try:
                                                        int_val = int(val.strip())
                                                        row.append(int_val)
                                                except ValueError:
                                                        row.append(val.strip())

					if type == 'NODE':
						nodes.append(row)
					elif type == 'JOB':
						jobs.append(row)
					elif type == 'NEW_LRMS_JOB':
						lrms_jobs.append(row)
					elif type == 'END_LRMS_JOB':
						for job in lrms_jobs:
							if row[JOB_RM_NAME] == job[JOB_RM_NAME] and row[JOB_ID] == job[JOB_ID]:
								job[JOB_END_TIME] = row[END_JOB_END_TIME]
								job[JOB_START_TIME] = row[END_JOB_START_TIME]
        finally:
                f.close()

        return (jobs, nodes, lrms_jobs)

def save_data_to_db(dbfile, jobs, nodes, lrms_jobs):
	try:
		db = DataBase(dbfile)
		db.connect()

		if len(jobs) > 0:
			if len(jobs[0]) == NUM_JOBS_DATA_FIELDS:
				for row in jobs:
					nrow = [float(time.mktime(row[0].timetuple()))]
					nrow.extend(row[1:])
					db.execute('insert into report_jobs values (?,?,?,?)', tuple(nrow))
			else:
				print "Incompatible jobs data format"
				return False
		else:
			print "No job info to import"

		if len(nodes) > 0:
			if len(nodes[0]) == NUM_NODES_DATA_FIELDS:
				for row in nodes:
					nrow = [float(time.mktime(row[0].timetuple()))]
					nrow.extend(row[1:])
					db.execute('insert into report_nodes values (?,?,?,?,?)', tuple(nrow))
			else:
				print "Incompatible nodes data format"
				return False
		else:
			print "No node info to import"

		if len(lrms_jobs) > 0:
			if len(lrms_jobs[0]) == NUM_LRMS_JOBS_DATA_FIELDS:
				for row in lrms_jobs:
					db.execute('insert into lrms_jobs values (?,?,?,?,?,?,?,?)', tuple(row))
			else:
				print "Incompatible lrms_jobs data format"
				return False
		else:
			print "No job info to import"

		db.close()
		return True

        except Exception, e:
                print e
                return False

def load_nodes_from_db(dbfile):
	table = []

	try:
		db = DataBase(dbfile)
		db.connect()

		res = db.select('''select * from report_nodes order by date''')
		
		for d in res:
			row = range(NUM_NODES_DATA_FIELDS)
			row[DATE] = datetime.fromtimestamp(float(d[DATE]))
			row[NODE_NAME] = d[NODE_NAME]
			row[STATUS] = int(d[STATUS])
			row[FREE_SLOTS] = int(d[FREE_SLOTS])
			row[TOTAL_SLOTS] = int(d[TOTAL_SLOTS])
			table.append(row)

		db.close()
	except Exception, e:
		print e

	return table

def load_jobs_from_db(dbfile):
	table = []

	try:
		db = DataBase(dbfile)
		db.connect()
			
		res = db.select('''select * from report_jobs order by date''')

		for d in res:
			row = range(NUM_JOBS_DATA_FIELDS)
			row[DATE] = datetime.fromtimestamp(float(d[DATE]))
			row[MANAGER] = d[MANAGER]
			row[NEW_JOB] = int(d[NEW_JOB])
			row[RELEASE_JOB] = int(d[RELEASE_JOB])
			table.append(row)

		db.close()
	except Exception, e:
		print e

	return table

def print_power_info(dbfile, init_date, end_date):
	try:
		db = DataBase(dbfile)
		db.connect()

		now = float(time.time())
		if end_date is None or end_date > now:
			end_date = now

		res = db.select('''select * from report_nodes where date <= %s order by date''' % (end_date))

		nodes = {}
		for d in res:
			date = float(d[DATE])
			node_name = d[NODE_NAME]
			total = int(d[TOTAL_SLOTS])
			free = int(d[FREE_SLOTS])
			status = int(d[STATUS])
			
			if node_name in nodes:
				(old_date, old_status, old_free, old_total, off, idle, used, full, fail) = nodes[node_name]
				if init_date is None or init_date <= date:
					if init_date is None or init_date <= old_date:
						delay = datetime.fromtimestamp(float(date)) - datetime.fromtimestamp(float(old_date))
					else:
						delay = datetime.fromtimestamp(float(date)) - datetime.fromtimestamp(float(init_date))

					delay -= timedelta(microseconds=delay.microseconds)
		
					if old_status == OFF or old_status == POWOFF or old_status == BOOTING or old_status == DOWN:
						# Is Off
						off += delay
					elif old_status == BUSY:
						# Is busy
						full += delay
					elif old_status == AVAILABLE:
						if (old_free == old_total):
							# The node is idle
							idle += delay
						elif (old_free == 0):
							# The node totally full
							full += delay
						else:
							# The node is partially used
							used += delay
					else: # the node is failing
						fail += delay
						
				nodes[node_name] = (date, status, free, total, off, idle, used, full, fail)
			else:
				nodes[node_name] = (date, status, free, total, timedelta(0), timedelta(0), timedelta(0), timedelta(0), timedelta(0))

		db.close()
		
		time_processed = timedelta(0)
		avail_power = 0.0
		busy_power = 0.0
		off_power = 0.0
		avail_power_no_clues = 0.0
		off_power_no_clues = 0.0
		
		pct_off_total = 0.0
		pct_idle_total = 0.0
		pct_used_total = 0.0
		pct_full_total = 0.0
		pct_fail_total = 0.0
		
		for node, v in sorted(nodes.iteritems()):
			(last_date, last_status, last_free, last_total, off, idle, used, full, fail) = v
			if init_date is not None and last_date < init_date:
				last_date = init_date
			delay = datetime.fromtimestamp(float(end_date)) - datetime.fromtimestamp(float(last_date))
			delay -= timedelta(microseconds=delay.microseconds)
			if last_status == OFF or last_status == POWOFF or last_status == BOOTING or last_status == DOWN:
				off += delay
			elif last_status == BUSY:
				# Is busy
				full += delay
			elif last_status == AVAILABLE:
				if (last_free == last_total):
					# The node is idle
					idle += delay
				elif (last_free == 0):
					# The node totally full
					full += delay
				else:
					# The node is partially used
					used += delay
			else:
				fail += delay

			total = idle + off + used + full + fail
			if total > time_processed:
				time_processed = total
				
			pct_off = float(td_total_seconds(off)) / float(td_total_seconds(total)) * 100.0
			pct_idle = float(td_total_seconds(idle)) / float(td_total_seconds(total)) * 100.0
			pct_used = float(td_total_seconds(used)) / float(td_total_seconds(total)) * 100.0
			pct_full = float(td_total_seconds(full)) / float(td_total_seconds(total)) * 100.0
			pct_fail = float(td_total_seconds(fail)) / float(td_total_seconds(total)) * 100.0
			
			pct_off_total += pct_off
			pct_idle_total += pct_idle
			pct_used_total += pct_used
			pct_full_total += pct_full
			pct_fail_total += pct_fail

			node_avail_power = (td_total_seconds(idle) / 3600.0) * get_node_pow_info(node, 'NODE_IDLE_POW') / 1000.0
			node_busy_power = (td_total_seconds(used+full) / 3600.0) * get_node_pow_info(node, 'NODE_USED_POW') / 1000.0
			node_off_power = (td_total_seconds(off+fail) / 3600.0) * get_node_pow_info(node, 'NODE_OFF_POW') / 1000.0
			
			avail_power += node_avail_power
			busy_power += node_busy_power
			off_power += node_off_power
			
			node_avail_power_no_clues = (td_total_seconds(idle+off) / 3600.0) * get_node_pow_info(node, 'NODE_IDLE_POW') / 1000.0
			node_off_power_no_clues = (td_total_seconds(fail) / 3600.0) * get_node_pow_info(node, 'NODE_OFF_POW') / 1000.0
			
			avail_power_no_clues += node_avail_power_no_clues
			off_power_no_clues += node_off_power_no_clues
		
		pct_off_total /= float(len(nodes))	
		pct_idle_total /= float(len(nodes))
		pct_used_total /= float(len(nodes))
		pct_full_total /= float(len(nodes))
		pct_fail_total /= float(len(nodes))
		
		base_power = MIN_POW * (td_total_seconds(time_processed) / 3600.0) / 1000.0
		total_power = base_power + avail_power + busy_power + off_power
		total_cost = total_power * ENERGY_COST
		
		print "Time processed:"
		print time_processed
		print "Estimated Power Consumption (With CLUES)"
		print "Base Power\tAvail Power\tBusy Power\tOff Power\tTotal Power\tTotal Cost"
		msg = "%10.2f kWh\t%10.2f kWh\t%10.2f kWh\t%10.2f kWh\t%10.2f kWh\t%8.2f %s" % (base_power, avail_power, busy_power, off_power, total_power, total_cost, CURRENCY)
		print msg
		msg = "%10.2f %%\t%10.2f %%\t%10.2f %%\t%10.2f %%" % (100, pct_idle_total, pct_used_total + pct_full_total, pct_off_total + pct_fail_total)
		print msg
		
		total_power_no_clues = base_power + avail_power_no_clues + busy_power + off_power_no_clues
		total_cost_no_clues = total_power_no_clues * ENERGY_COST

		print "Estimated Power Consumption (Without CLUES)"
		print "Base Power\tAvail Power\tBusy Power\tOff Power\tTotal Power\tTotal Cost"
		msg = "%10.2f kWh\t%10.2f kWh\t%10.2f kWh\t%10.2f kWh\t%10.2f kWh\t%8.2f %s" % (base_power, avail_power_no_clues, busy_power, off_power_no_clues, total_power_no_clues, total_cost_no_clues, CURRENCY)
		print msg
		msg = "%10.2f %%\t%10.2f %%\t%10.2f %%\t%10.2f %%" % (100, pct_off_total + pct_idle_total, pct_used_total + pct_full_total, pct_fail_total)
		print msg
		
		power_save = total_power_no_clues - total_power
		economic_save = total_cost_no_clues - total_cost
		pct_save = (economic_save / total_cost_no_clues) * 100.0

		print "Estimated Savings"
		msg = "%10.2f kWh\t%8.2f %s\t%6.2f%%" % (power_save, economic_save, CURRENCY, pct_save)
		print msg

	except Exception, e:
		print e


def print_jobs_info(dbfile, init_date, end_date):
	try:
		queued_jobs = {}
		job_count = {}
		job_wait = {}
		wait_time = {}
		seq_jobs = {}
		par_jobs = {}
		nodes_par_jobs = {}
		
		db = DataBase(dbfile)
		db.connect()
		
		sql = "select * from report_jobs"
		if end_date is not None or init_date is not None:
			sql = sql + " where "
			if end_date is not None:
				sql = sql + "date <= %f" % end_date
			if end_date is not None and init_date is not None:
				sql = sql + " and "
			if init_date is not None:
				sql = sql + "date >= %f" % init_date
		sql = sql + " order by date"

		res = db.select(sql)

		cont = 0
		for d in res:
			row = range(NUM_JOBS_DATA_FIELDS)
			row[DATE] = datetime.fromtimestamp(float(d[DATE]))
			row[MANAGER] = d[MANAGER]
			row[NEW_JOB] = int(d[NEW_JOB])
			row[RELEASE_JOB] = int(d[RELEASE_JOB])
			
			if cont == 0:
				first = row[DATE]

			# a job reset
			if row[NEW_JOB] < 0:
				queued_jobs = {}
			else:
				if row[NEW_JOB] > 0:
					if not row[MANAGER] in job_count.keys():
						job_count[row[MANAGER]] = 0
					job_count[row[MANAGER]] += 1
					if not row[MANAGER] in queued_jobs.keys():
						queued_jobs[row[MANAGER]] = []
					queued_jobs[row[MANAGER]].append(row)
					if row[NEW_JOB] == 1:
						if not row[MANAGER] in seq_jobs.keys():
							seq_jobs[row[MANAGER]] = 0
						seq_jobs[row[MANAGER]] += 1
					else:
						if not row[MANAGER] in par_jobs.keys():
							par_jobs[row[MANAGER]] = 0
						par_jobs[row[MANAGER]] += 1
						if not row[MANAGER] in nodes_par_jobs.keys():
							nodes_par_jobs[row[MANAGER]] = 0
						nodes_par_jobs[row[MANAGER]] += row[NEW_JOB]
				if row[RELEASE_JOB] > 0:
					qj = queued_jobs[row[MANAGER]].pop(0)
					diff_time = row[0] - qj[0]
					# los trabajos que no esperan son liberados antes de 5 segs
					of = timedelta(seconds=5)
					if diff_time > of:
						if not row[MANAGER] in job_wait.keys():
							job_wait[row[MANAGER]] = 0
						job_wait[row[MANAGER]] += 1
						if not row[MANAGER] in wait_time.keys():
							wait_time[row[MANAGER]] = timedelta()
						wait_time[row[MANAGER]] += diff_time
			cont += 1

		managers = par_jobs.keys()
		if len(managers) == 0:
			managers = seq_jobs.keys()

		if cont == 0 or len(managers) == 0:
			print "No data available"
		else:
			last = datetime.now()

			if end_date is not None:
				last = datetime.fromtimestamp(float(end_date))
			if init_date is not None:
				first = datetime.fromtimestamp(float(init_date))
			
			delay = last - first
			
			print "Time processed:"
			print delay

			for manager in managers:
				print "Manager: " + manager

				num_par_jobs = 0
				num_seq_jobs = 0
				num_nodes_par_jobs = 0
				num_job_wait = 0
				num_wait_time = 0
				num_job_count = 0
				if manager in par_jobs.keys():
					num_par_jobs = par_jobs[manager]
				if manager in seq_jobs.keys():
					num_seq_jobs = seq_jobs[manager]
				if manager in nodes_par_jobs.keys():
					num_nodes_par_jobs = nodes_par_jobs[manager]
				if manager in job_wait.keys():
					num_job_wait = job_wait[manager]
				if manager in wait_time.keys():
					num_wait_time = wait_time[manager]
				if manager in job_count.keys():
					num_job_count = job_count[manager]

				# busts of jobs
				if (num_par_jobs > 0):
					mean_nodes_par_jobs = float(num_nodes_par_jobs) / float(num_par_jobs)
				else:
					mean_nodes_par_jobs = 0
				msg = ("Total Processed Jobs: %d: %d sequential, %d parallel (mean node number of parallel jobs: %.2f)" 
					% (num_job_count, num_seq_jobs, num_par_jobs, mean_nodes_par_jobs))
				print msg
				if job_count[manager] > 0:
					pct = float(num_job_wait) / float(num_job_count) * 100.0
				else:
					pct = 0
				msg = "Jobs that must wait: %d (%.2f%% of total jobs)" % (num_job_wait, pct)
				print msg
				if num_job_wait > 0:
					msg = "Mean wait time: %s" % (num_wait_time / num_job_wait)
				else:
					msg = "Mean wait time: 0"
				print msg

		db.close()
	except Exception, e:
		print e

def print_node_info(dbfile, init_date, end_date):
	try:
		db = DataBase(dbfile)
		db.connect()
		
		now = float(time.time())
		if end_date is None or end_date > now:
			end_date = now

		res = db.select('''select * from report_nodes where date <= %s order by date''' % (end_date))

		nodes = {}
		for d in res:
			date = float(d[DATE])
			node_name = d[NODE_NAME]
			total = int(d[TOTAL_SLOTS])
			free = int(d[FREE_SLOTS])
			status = int(d[STATUS])
			
			if node_name in nodes:
				(old_date, old_status, old_free, old_total, off, idle, used, full, fail, num_switch_on) = nodes[node_name]
				
				if init_date is None or init_date <= date:
					if init_date is None or init_date <= old_date:
						delay = datetime.fromtimestamp(float(date)) - datetime.fromtimestamp(float(old_date))
					else:
						delay = datetime.fromtimestamp(float(date)) - datetime.fromtimestamp(float(init_date))

					delay -= timedelta(microseconds=delay.microseconds)
	
					if old_status == OFF or old_status == POWOFF or old_status == BOOTING or old_status == DOWN:
						# Is Off
						off += delay
					elif old_status == BUSY:
						# Is busy
						full += delay
					elif old_status == AVAILABLE:
						if (old_free == old_total):
							# The node is idle
							idle += delay
						elif (old_free == 0):
							# The node totally full
							full += delay
						else:
							# The node is partially used
							used += delay
					else: # the node is failing
						fail += delay
							
					if old_status > 1 and status <= 1:
						num_switch_on += 1
					
				nodes[node_name] = (date, status, free, total, off, idle, used, full, fail, num_switch_on)
			else:
				nodes[node_name] = (date, status, free, total, timedelta(0), timedelta(0), timedelta(0), timedelta(0), timedelta(0), 0)

		db.close()
				
		msg = "Nodename            Off    Idle   P.Used    Full   Fail   Switch On"
		print msg

		time_processed = timedelta(0)
		pct_off_total = 0.0
		pct_idle_total = 0.0
		pct_used_total = 0.0
		pct_full_total = 0.0
		pct_fail_total = 0.0
		for node, v in sorted(nodes.iteritems()):
			(last_date, last_status, last_free, last_total, off, idle, used, full, fail, num_switch_on) = v
			
			if init_date is not None and last_date < init_date:
				last_date = init_date
			
			delay = datetime.fromtimestamp(float(end_date)) - datetime.fromtimestamp(float(last_date))
			delay -= timedelta(microseconds=delay.microseconds)
			if last_status == OFF or last_status == POWOFF or last_status == BOOTING or last_status == DOWN:
				off += delay
			elif last_status == BUSY:
				# Is busy
				full += delay
			elif last_status == AVAILABLE:
				if (last_free == last_total):
					# The node is idle
					idle += delay
				elif (last_free == 0):
					# The node totally full
					full += delay
				else:
					# The node is partially used
					used += delay
			else:
				fail += delay

			total = idle + off + used + full + fail
			
			if total > time_processed:
				time_processed = total
			
			pct_off = float(td_total_seconds(off)) / float(td_total_seconds(total)) * 100.0
			pct_idle = float(td_total_seconds(idle)) / float(td_total_seconds(total)) * 100.0
			pct_used = float(td_total_seconds(used)) / float(td_total_seconds(total)) * 100.0
			pct_full = float(td_total_seconds(full)) / float(td_total_seconds(total)) * 100.0
			pct_fail = float(td_total_seconds(fail)) / float(td_total_seconds(total)) * 100.0
			msg = "%6.2f%% %6.2f%% %6.2f%% %6.2f%% %6.2f%% %5d" % (pct_off, pct_idle, pct_used, pct_full, pct_fail, num_switch_on)
			print node.ljust(16), msg
			pct_off_total += float(td_total_seconds(off))
			pct_idle_total += float(td_total_seconds(idle))
			pct_used_total += float(td_total_seconds(used))
			pct_full_total += float(td_total_seconds(full))
			pct_fail_total += float(td_total_seconds(fail))
		
		total = float(pct_off_total + pct_idle_total + pct_used_total + pct_full_total + pct_fail_total)
		pct_off_total = (pct_off_total / total) * 100
		pct_idle_total = (pct_idle_total / total) * 100
		pct_used_total = (pct_used_total / total) * 100
		pct_full_total = (pct_full_total / total) * 100
		pct_fail_total = (pct_fail_total / total) * 100
		print "\nTime processed:"
		print time_processed
		print "\nMean Values:"
		msg = " Off\tIdle\tP.Used\tFull\tFail"
		print msg
		msg = "%6.2f%%\t%6.2f%%\t%6.2f%%\t%6.2f%%\t%6.2f%%" % (pct_off_total, pct_idle_total, pct_used_total, pct_full_total, pct_fail_total)
		print msg

	except Exception, e:
		print e

def td_total_seconds(td):
	return ((td.seconds + td.days * 24 * 3600) * 10**6) / 10**6

def print_lrms_jobs_info(dbfile, init_date, end_date):
	try:
		db = DataBase(dbfile)
		db.connect()
		now = float(time.time())
		if end_date > now:
			end_date = now

		sql = "select * from lrms_jobs"
		if end_date is not None or init_date is not None:
			sql = sql + " where "
			if end_date is not None:
				sql = sql + "StartTime <= %f" % end_date
			if end_date is not None and init_date is not None:
				sql = sql + " and "
			if init_date is not None:
				sql = sql + "StartTime >= %f" % init_date

		res = db.select(sql)

		job_time = {}
		cont = {}
		whole_cluster_jobs = {}
		for d in res:
			if not d[JOB_RM_NAME] in job_time.keys():
				job_time[d[JOB_RM_NAME]] = 0
			if d[JOB_END_TIME] != None:
				job_time[d[JOB_RM_NAME]] += d[JOB_END_TIME] - d[JOB_START_TIME]
			if not d[JOB_RM_NAME] in whole_cluster_jobs.keys():
				whole_cluster_jobs[d[JOB_RM_NAME]] = 0
			if d[JOB_WHOLE_CLUSTER]:
				whole_cluster_jobs[d[JOB_RM_NAME]] += 1
			if not d[JOB_RM_NAME] in cont.keys():
				cont[d[JOB_RM_NAME]] = 0
			cont[d[JOB_RM_NAME]] += 1

		for manager in cont.keys():
			avg_job_time = timedelta(seconds=job_time[manager] / float(cont[manager]))
			print "Manager: " + manager
			print "Total LRMS jobs: " + str(cont[manager])
			print "Avg. job time: " + str(avg_job_time)
			print "Total whole cluster jobs: " + str(whole_cluster_jobs[manager])

		db.close()
	except Exception, e:
		print e

def generate_states(dbfile, init_date, end_date, delay):
	try:
		db = DataBase(dbfile)
		db.connect()
		result = db.select('''select distinct(Node) from report_nodes''')
		
		nodes = {}
		for d in result:
			nodes[d[0]] = (None, None, None, None)
			
		now = float(time.time())
		if end_date == None or end_date > now:
			end_date = now

		result = db.select('''select * from report_nodes where date <= %s order by date''' % (end_date))

		res = []
		curr_date = init_date
		for d in result:
			if curr_date == None:
				curr_date = d[DATE]

			while d[DATE] > curr_date:
				row = [curr_date]
				for nodename in nodes.keys():
					(date, state, slots, total) = nodes[nodename]
					row.append((nodename, state, slots, total))
				res.append(row)
				curr_date += delay * 60 # delay in minutes

			nodes[d[NODE_NAME]] = (d[DATE], d[STATUS], d[FREE_SLOTS], d[TOTAL_SLOTS])

		# repeat for the last row
		if len(result) > 0:
			while d[DATE] > curr_date:
				row = [curr_date]
				for nodename in nodes.keys():
					(date, state, slots, total) = nodes[nodename]
					row.append((nodename, state, slots, total))
				res.append(row)
				curr_date += delay * 60 # delay in minutes

		db.close()

		return (nodes.keys(), res)
	except Exception, e:
		print e


def print_csv_states(res):
	print "date;Full;Used;Idle;Off;Error"
	
	for elem in res:
		Full=0
		Used=0
		Idle=0
		Off=0
		Error=0
		for data in elem[1:]:
			(nodename, state, slots, total) = data
			
			if state == AVAILABLE:
				if slots == total:
					Idle+=1
				elif slots > 0:
					Used+=1
				else:
					Full+=1
			elif state == BUSY:
				Full+=1
			elif state == OFF or state == BOOTING or state == POWOFF or state == DOWN or state == OFFLINE:
				Off+=1
			else:
				Error+=1
			
		msg = "%s;%d;%d;%d;%d;%d" % (str(datetime.fromtimestamp(float(elem[0]))),Full,Used,Idle,Off,Error)
		print msg


def print_jobs_cvs(dbfile, init_date, end_date, delay):
        #try:
                db = DataBase(dbfile)
                db.connect()
                now = float(time.time())
                if end_date > now:
                        end_date = now

		sql = "select * from lrms_jobs where QueuedTime NOT NULL and StartTime NOT NULL and QueuedTime < %f and EndTime NOT NULL and EndTime > %f order by QueuedTime asc" % (end_date, init_date)

                res = db.select(sql)

                active_jobs = []
                curr_date = init_date
                for d in res:
                        start_time = d[JOB_QUEUE_TIME]

			diff = datetime.fromtimestamp(d[JOB_START_TIME]) - datetime.fromtimestamp(d[JOB_QUEUE_TIME])
			if diff.days > 10:
				continue

                        while curr_date <= start_time:
                                tmp_active_jobs = []
                                tmp_active_jobs.extend(active_jobs)
                                for job in tmp_active_jobs:
                                        if job[JOB_END_TIME] < curr_date:
                                                active_jobs.remove(job)
                                msg = str(datetime.fromtimestamp(float(curr_date))) + "; "
				total=0
				for elem in active_jobs:
					nodes = elem[JOB_NODES]
					if elem[JOB_SPECS]:
						nodes*=2
					total += nodes
                                msg += str(total) + ";"
                                msg += str(len(active_jobs))
                                print msg
                                curr_date += delay * 60 # delay in minutes

                        active_jobs.append(d)

                db.close()
        #except Exception, e:
        #       print e

def print_jobs_cvs1(dbfile, init_date, end_date, delay):
	#try:
		queued_jobs = {}
		job_count = {}
		job_wait = {}
		wait_time = {}
		seq_jobs = {}
		par_jobs = {}
		nodes_par_jobs = {}
		
		db = DataBase(dbfile)
		db.connect()
		
		sql = "select * from report_jobs"
		if end_date is not None or init_date is not None:
			sql = sql + " where "
			if end_date is not None:
				sql = sql + "date <= %f" % end_date
			if end_date is not None and init_date is not None:
				sql = sql + " and "
			if init_date is not None:
				sql = sql + "date >= %f" % init_date
		sql = sql + " order by date"

		res = db.select(sql)

		total = 0
		curr_date = init_date
		for d in res:
			row = range(NUM_JOBS_DATA_FIELDS)
			row[DATE] = d[DATE]
			row[MANAGER] = d[MANAGER]
			row[NEW_JOB] = int(d[NEW_JOB])
			row[RELEASE_JOB] = int(d[RELEASE_JOB])
			
			if row[NEW_JOB] > 0:
				if not row[MANAGER] in job_count.keys():
					job_count[row[MANAGER]] = 0
					
			while curr_date <= row[DATE]:
				msg = str(datetime.fromtimestamp(float(curr_date))) + "; "
				for rm in job_count:
					total += job_count[rm] 
					msg += rm + "; " + str(job_count[rm]) + " "
					job_count[rm] = 0
				print msg
				curr_date += delay * 60 # delay in minutes
			
			if row[NEW_JOB] > 0:
				job_count[row[MANAGER]] += 1

		db.close()
	#except Exception, e:
	#	print e


def gen_host_graph(nodes, res, ini, end, delay, filename, rrd_file, width = 800, height = 400):
	# get initial and end date
	START = None
	END = None
	if ini is not None:
	    START = int(ini)-1
	else:
		if len(res) > 0:
			START = int(res[0][0])-1
	
	if end is not None:
		END = int(end)
	else:
		if len(res) > 0:
			END = int(res[len(res)-1][0])

	if START is None:
		START = END - 1
		
	if END is None:
		END = START - 1

	if START is None or END is None:
		return
	
	# create the rrd file
	rrdtool.create(rrd_file,
		       '--step', str(delay*60),
		       'DS:idle:GAUGE:' + str(delay*60*2) + ':0:' + str(nodes),
		       'DS:used:GAUGE:' + str(delay*60*2) + ':0:' + str(nodes),
		       'DS:full:GAUGE:' + str(delay*60*2) + ':0:' + str(nodes),
		       'DS:off:GAUGE:' + str(delay*60*2) + ':0:' + str(nodes),
		       'RRA:MAX:0.5:1:' + str(len(res)),
		       '--start', str(START))
	
	# add the data to the rrd file
	for elem in res:
		Full=0
		Used=0
		Idle=0
		Off=0
		Error=0
		
		for data in elem[1:]:
			(node, state, slots, total) = data
			
			if state == AVAILABLE:
				if slots == total:
					Idle+=1
				elif slots > 0:
					Used+=1
				else:
					Full+=1
			elif state == BUSY:
				Full+=1
			elif state == OFF or state == BOOTING or state == POWOFF or state == DOWN or state == OFFLINE:
				Off+=1
			else:
				Error+=1
	
		date = str(int(elem[0]))
		# Hago la suma para luego mostrar la grafica apilada
		# porque si uso las opciones STACK del rrdtool no queda bien
		full = str(Full)
		used = str(Full+Used)
		idle = str(Idle+Full+Used)
		off = str(Off+Error+Full+Used+Idle)
		
		rrdtool.update(rrd_file,
		       '--template', 'full:used:idle:off', date + ':' + full + ':' + used + ':' + idle + ':' + off)
	
	rrdtool.graph('-w', str(width),
			'-h', str(height),
			'-y', '1:1 1',
			filename,
			'--title', '"Hosts"',
			'--vertical-label', '"Number of hosts"',
			'--slope-mode',
			'--start', str(START),
			'--end', str(END),
			'DEF:full=' + rrd_file + ':full:MAX',
			'DEF:used=' + rrd_file + ':used:MAX',
			'DEF:idle=' + rrd_file + ':idle:MAX',
			'DEF:off=' + rrd_file + ':off:MAX',
			'AREA:off#ffffff:"nodes off"',
			'AREA:idle#379f37:"nodes idle"',
			'AREA:used#f89b2e:"nodes partially used"',
			'AREA:full#cd5555:"busy nodes"',
			)

if __name__=="__main__":
	parser = OptionParser(usage="%prog [-t|--time-step] [-f|--db-file filename] [-i|--initial-date date] [-e|--end-date date] <power|jobinfo|nodeinfo|report|readlog|host_graph [image_file] [rrd_file] [width] [height]> "+NOTICE, version="%prog 1.0")
	parser.add_option("-t", "--time-step", dest="time_step", nargs=1, default=120, help="Time step in minutes (for the report and host_graph ops)", type="int")
	parser.add_option("-i", "--initial-date", dest="ini_date", nargs=1, default=None, help="Initial date to process data", type="string")
	parser.add_option("-e", "--end-date", dest="end_date", nargs=1, default=None, help="End date to process data", type="string")
	parser.add_option("-f", "--db-file", dest="db_file", nargs=1, default=CLUES_DB, help="DB File to query instead of the CLUES_DB file defined in the config file", type="string")
	(options, args) = parser.parse_args()

	if len(args) == 0:
		parser.error("no operation provided")

	operation = args[0].lower()
	if (operation not in ["power", "jobinfo", "nodeinfo", "readlog", "report", "host_graph"]):
		parser.error("operation not recognised")

	ini = None
	end = None
	if options.ini_date != None:
		ini = time.mktime(time.strptime(options.ini_date, "%d-%m-%Y"))
	if options.end_date != None:
		end = time.mktime(time.strptime(options.end_date, "%d-%m-%Y"))

	if (operation == "readlog"):
		table = load_jobs_from_db(options.db_file)
		if len(table) > 0:
			init_date = table[len(table)-1][DATE]
		else:
			init_date = None
		if len(table) > 1:
			init_date = table[0][DATE]
		else:
			init_date = None
		(jobs, nodes, lrms_jobs) = load_data_from_logfiles(init_date)
		report_init(options.db_file)
		save_data_to_db(options.db_file, jobs, nodes, lrms_jobs)

	if (operation == "jobinfo"):
		print_jobs_info(options.db_file, ini, end)
		print ""
		print_lrms_jobs_info(options.db_file, ini, end)

	if (operation == "power"):
		print_power_info(options.db_file, ini, end)
		
	if (operation == "nodeinfo"):
		print_node_info(options.db_file, ini, end)

	if (operation == "report"):
		#(nodes, res) = generate_states(options.db_file, ini, end, options.time_step)
		#print_csv_states(res)
		print_jobs_cvs(options.db_file, ini, end, options.time_step)
		
	if (operation == "host_graph"):
		import rrdtool

		if len(args) < 2:
			parser.error("no filename provided to host_graph operation")

		filename = args[1]
		
		width = 800
		height = 400
		if len(args) > 2:
			if len(args) > 3:
				width = int(args[2])
				height = int(args[3])
			else:
				parser.error("You must specify width and height")
				
		rrd_file = 'hosts_graph.rrd'
		if len(args) > 4:
			rrd_file = args[4]
			
		(nodes, res) = generate_states(options.db_file, ini, end, options.time_step)
		gen_host_graph(len(nodes), res, ini, end, options.time_step, filename, rrd_file, width, height)
