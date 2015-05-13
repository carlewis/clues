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

import sys
import string
import xmlrpclib
import logging
from config import *
from optparse import OptionParser
import auth
import os
import getpass
import time
from datetime import timedelta
try:
	from config_local import *
except:
	pass

NOTICE="\n\n\
CLUES - Cluster Energy Saving System\n\
Copyright (C) 2011 - GRyCAP - Universitat Politecnica de Valencia\n\
This program comes with ABSOLUTELY NO WARRANTY; for details please\n\
read the terms at http://www.gnu.org/licenses/gpl-3.0.txt.\n\
This is free software, and you are welcome to redistribute it\n\
under certain conditions; please read the license at \n\
http://www.gnu.org/licenses/gpl-3.0.txt for details."

class AuthHelper:
	def __init__(self, username = None):
		self.token_filename = "~/.clues/clues_token"
		# intentamos iniciar, pero con el metodo de las credenciales locales
		try:
			self.passwd_filename = os.environ['CLUES_AUTH']
		except:
			self.passwd_filename = "~/.clues/clues_auth"
		if username is None:
			print "using login name because no user was specified in commandline"
			username = os.getlogin()
		self.username = username
	def init_from_token_file(self):
		if not SECURITY_TOKEN_ENABLED:
			logging.debug("security disabled in configuration")
			return True, ""

		return auth.AuthClient.get_token_session_from_file(self.token_filename, self.username)

	def init_from_passwd_file(self):
		if not SECURITY_TOKEN_ENABLED:
			logging.debug("security disabled in configuration")
			return True, ""

		return auth.AuthClient.get_token_pass_from_file(self.passwd_filename, self.username)

	def init_from_passwd(self, proxy):
		if not SECURITY_TOKEN_ENABLED:
			logging.debug("security disabled in configuration")
			return True, ""

		client = auth.AuthClient(proxy)
		password = getpass.getpass("Password (%s): " % self.username)
		return client.init_session_by_pass(self.token_filename, self.username, password) 


if __name__ == "__main__":
	CLUES_XML_RPC = 'http://%s:%s/RPC2' %(SERVER_HOST,SERVER_PORT)

	parser = OptionParser(usage="%prog [-u <user>] [-d|--daemon-xmlrpc-url] [-s|--state <state>] <enable|disable|status|poweron|poweroff> <node 1> <node 2> ..."+NOTICE, version="%prog "+CLUES_VERSION_TAG)
	parser.add_option("-d", "--daemon-xmlrpc-url", dest="clues_xmlrpc", nargs=1, default=CLUES_XML_RPC, help="xmlrpc url of CLUES daemon", type="string")
	parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="shows more information")
	parser.add_option("-u", "--user", dest="user", nargs=1, default=None, help="username to accede clues server", type="string")
	parser.add_option("-s", "--state", dest="state", nargs=1, default=None, help="filter the nodes by their state", type="string")

	(options, args) = parser.parse_args()
	CLUES_XML_RPC = options.clues_xmlrpc
	LOGLEVEL = logging.INFO
	if (options.verbose):
		LOGLEVEL = logging.DEBUG

	logging.basicConfig(level=LOGLEVEL,
				 format='%(message)s',
				 datefmt='%m-%d %H:%M:%S',
				 filename=None,
				 filemode='a')

	if len(args) == 0:
		parser.error("no operation provided")

	operation = args[0].lower()
	args = args[1:]

	if (operation not in ["enable", "disable", "status", "poweron", "poweroff"]):
		parser.error("operation not recognised")

	auth_helper = AuthHelper(options.user)
	if len(args) == 0:
		if (operation == "status"):
			try:
				server = xmlrpclib.ServerProxy(CLUES_XML_RPC)
				success, token_session = auth_helper.init_from_token_file()
				if SECURITY_TOKEN_ENABLED:
					if success:
						success, info = server.node_info(token_session)
						if success:
							logging.debug("authenticated using token file")
					if not success:
						success, token_session = auth_helper.init_from_passwd_file()
						if success:
							success, info = server.node_info(token_session)
							if success:
								logging.debug("authenticated using password file")
					if not success:
						success, token_session = auth_helper.init_from_passwd(server)
						if success:
							success, info = server.node_info(token_session)
				else:
					success, info = server.node_info("")

				if success:
					for x in info:
						print '   Info about LRMS %s' %x[0]
						print 'Node            State                Free  Total Idle'
						print '-------------------------------------------------------------'
						for node in x[1]:
							if options.state is None or node[1].find(options.state) != -1:
								now = time.time()
								time_idle = now - node[4]
								if time_idle <= TIME_UPDATE_STATUS:
									time_idle = 0
								if node[1].find("off") == -1 and node[1].find("disabled") == -1:
									str_idle = str(timedelta(seconds = int(time_idle)))
								else:
									str_idle = "-"
								
								# Mark the nodes near to switch off
								to_pow_off = ""
								if node[1].find("failed") == -1 and node[1].find("disabled") == -1 and node[1].find("off") == -1 and MAX_TIME_IDLE - int(time_idle) < 60:
									to_pow_off = " *"
								
								print node[0].ljust(16)[0:15], node[1].ljust(20) , str(int(node[2])).ljust(5), str(int(node[3])).ljust(5), str_idle.ljust(8), to_pow_off
						print
				else:
					print 'not authorized'

			except Exception, e:
				logging.error("could not connect to CLUES daemon (%s)" % e)
		else:
			parser.error("few arguments for this operation")
	else:
		token_session = ""
		try:
			# This way of authenticating makes the client to authenticate twice, but
			# in this case, it makes esasier to understand the code
			server = xmlrpclib.ServerProxy(CLUES_XML_RPC)
			if operation in ["enable", "disable", "poweron", "poweroff"]:
				if SECURITY_TOKEN_ENABLED:
					success, token_session = auth_helper.init_from_token_file()
					if success:
						success, token_session = server.init_session(token_session)
						if success:
							logging.debug("authenticated using token file")
					if not success:
						success, token_session = auth_helper.init_from_passwd_file()
						if success:
							success, token_session = server.init_session(token_session)
							if success:
								logging.debug("authenticated using password file")
					if not success:
						success, token_session = auth_helper.init_from_passwd(server)
						if success:
							success, token_session = server.init_session(token_session)

					if not success:
						logging.error("user not authorized to perform the operation")
						sys.exit(1)
				else:
					token_session = ""
		except Exception, e:
			logging.error("could not connect to CLUES daemon (%s)" % e)
			sys.exit(1)

		if operation in ["enable", "disable"]:
			exclude = True
			if (operation == "enable"):
				exclude = False
			elif (operation == "disable"):
				exclude = True
			try:
				for node in args:
					if exclude:
						logging.debug("exluding node %s" % node)
					else:
						logging.debug("enabling node %s" % node)
					(result, answer) = server.exclude_node(token_session, node, exclude)
					if (not result):
						logging.error(answer)
					else:
						logging.debug(answer)
			except Exception, e:
				logging.error("could not connect to CLUES daemon (%s)" % e)

		elif operation in ["poweron", "poweroff"]:
			poweron = True
			if (operation == "poweron"):
				poweron = True
			elif (operation == "poweroff"):
				poweron = False
			try:
				for node in args:
					if poweron:
						logging.debug("powering on node %s" % node)
					else:
						logging.debug("powering off node %s" % node)
					(result, answer) = server.user_power(token_session, node, poweron)
					if (not result):
						logging.error(answer)
					else:
						logging.debug(answer)
			except Exception, e:
				logging.error("could not connect to CLUES daemon (%s)" % e)
		else:
			parser.error("too arguments for this operation")
		
	sys.exit(0)
