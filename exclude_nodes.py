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

import logging
from clues_daemon import CLUES_DB, NAME, STATE
from db import DataBase, IntegrityError

def get_node_list(managers):
	nodes = []
	for rm in managers.values():
		new_nodes = [item[NAME] for item in rm.node_table if item[NAME] not in nodes]
		nodes = nodes + new_nodes
		
	return nodes
	
def get_node_list_and_state(managers):
	nodes = {}
	for rm in managers.values():
		for item in rm.node_table:
			if nodes.has_key(item[NAME]):
				if (nodes[item[NAME]] is not None):
					if (nodes[item[NAME]] != item[STATE]):
						nodes[item[NAME]] = None
					else:
						nodes[item[NAME]] = item[STATE]
			else:
				nodes[item[NAME]] = item[STATE]
			nodes[item[NAME]]
	return nodes

def initially_excluded_nodes():
	return initly_excluded

if DataBase.db_available:
	# include a new nodename in the disabled database
	def disable_node(node_name):
		try:
			db = DataBase(CLUES_DB)
			db.connect()
			db.execute('''insert into disabled_hosts (hostname) values ("''' + node_name + '''")''')
			db.close()
			return True
		except IntegrityError:
			# ya estaba creado el nodo
			return True
		except:
			logging.error("failed inserting node %s in the disabled node list")
			return False

	# delete a nodename from the disabled database
	def enable_node(node_name):
		try:
			db = DataBase(CLUES_DB)
			db.connect()
			db.execute('''delete from disabled_hosts where hostname="''' + node_name + '''"''')
			db.close()
			return True
		except:
			logging.error("failed inserting node %s in the disabled node list")
			return False

	# get the combined list of excluded nodes
	def excluded_nodes():
		exclusion_list = initly_excluded
		hosts = []
		db = DataBase(CLUES_DB)
		db.connect()
		try:
			if not db.table_exists("disabled_hosts"):
				# la tabla no existe y la vamos a crear
				db.execute('''create table "disabled_hosts" ("hostname" TEXT PRIMARY KEY)''')
				
			res = db.select('''select hostname from "disabled_hosts"''')
			hosts = [h[0] for h in res]

		except Exception, e:
			logging.error("failed trying to obtain persistent disabled node list")

		db.close()
		return exclusion_list + hosts
else:
	# include a new nodename in the disabled database
	runtime_exclusion_list = []
	def disable_node(node_name):
		if node_name not in runtime_exclusion_list:
			runtime_exclusion_list.append(node_name)
		return True

	# delete a nodename from the disabled database
	def enable_node(node_name):
		if node_name in runtime_exclusion_list:
			runtime_exclusion_list.remove(node_name)
			return True
		return False

	# get the combined list of excluded nodes
	def excluded_nodes():
		return initly_excluded + runtime_exclusion_list


def read_excluded_list(filename, allow_multiple_nodenames_in_line = False):
	# List of nodes initially excluded (i.e. listed in exclusion file)
	global initly_excluded
	initly_excluded = []
		
        if filename==None: return

	try:
		f_excluded = open(filename, "rt")
	except Exception, e:	
		logging.error("could not open %s file" % filename)
		return None

	for line in f_excluded:
		components = line.split("#")
		lhs = components[0].strip().split(" ")
		if not allow_multiple_nodenames_in_line and len(lhs) > 1:
			logging.error("incorrect format for the excluded nodelist file (it has more than 1 nodename per line)")
		else:
			for nodename in lhs:
				if nodename != "" and nodename not in initly_excluded:
					initly_excluded.append(nodename)
				
	f_excluded.close()
