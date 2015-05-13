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
from optparse import OptionParser
import auth
import os
import getpass
from config import *
import hashlib
try:
	from config_local import *
except:
	pass

from db import DataBase
if not DataBase.db_available:
	print "Database " + DataBase.DB_TYPE + " is a requirement for this application"
	sys.exit(1)

NOTICE="\n\n\
CLUES - Cluster Energy Saving System\n\
Copyright (C) 2011 - GRyCAP - Universitat Politecnica de Valencia\n\
This program comes with ABSOLUTELY NO WARRANTY; for details please\n\
read the terms at http://www.gnu.org/licenses/gpl-3.0.txt.\n\
This is free software, and you are welcome to redistribute it\n\
under certain conditions; please read the license at \n\
http://www.gnu.org/licenses/gpl-3.0.txt for details."

if __name__ == "__main__":
	parser = OptionParser(usage="\n\t%prog <create> <username> [ <semicolon-sepparated attribute list> ]\n\t%prog <delete> <username>\n\t%prog <list>"+NOTICE, version="%prog "+CLUES_VERSION_TAG)
	parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="shows more information")

	(options, args) = parser.parse_args()
	LOGLEVEL = logging.INFO
	if (options.verbose):
		LOGLEVEL = logging.DEBUG

	logging.basicConfig(level=LOGLEVEL,
				 format='%(message)s',
				 datefmt='%m-%d %H:%M:%S',
				 filename=None,
				 filemode='a')

	if len(args) == 0:
		parser.error("wrong number of parameters")

	operation = args[0].lower()
	
	if operation in ["create", "delete"]:
		credential_server = auth.AuthServer(AUTH_TOKEN_EXPIRE, CLUES_DB, True)

		if len(args) < 2:
			parser.error("wrong number of parameters")
		username = args[1].lower()
		if operation == "create":
			attr_list = []
			if len(args) > 3:
				parser.error("wrong number of parameters")
			if len(args) == 3:
				attr_list = args[2].lower()
				attr_list = attr_list.split(";")

			passwd1 = getpass.getpass("Password (%s): " % username)
			passwd2 = getpass.getpass("Repeat password (%s): " % username)
			if (passwd1 != passwd2):
				logging.error("passwords are different")
			else:
				hasher = hashlib.sha1()
				hasher.update(passwd1)
				password = hasher.hexdigest()
				success = credential_server.create_user(username, password, attr_list)
				if success:
					logging.info("user %s successfully created" % username)
				else:
					logging.error("could not create user %s" % username)
		if operation == "delete":
			if len(args) > 2:
				parser.error("wrong number of parameters")

			user, attr = credential_server.get_user_data_from_db(username)
			if user is None:
				logging.error("user %s does not exist" % username)
			else:
				confirm = raw_input("Are you sure to delete user %s? (y/N): " % username)
				if confirm == "y":
					credential_server.delete_user(username)
				else:
					logging.info("deletion of user %s cancelled" % username)
		
	elif operation in ["list"]:
		credential_server = auth.AuthServer(AUTH_TOKEN_EXPIRE, CLUES_DB, True)

		if len(args) != 1:
			parser.error("wrong number of parameters")

		success, user_list = credential_server.get_user_list()
		if not success:
			logging.error("could not retrieve user list")
		else:
			print "user name\tattribute"
			print "-------------------------"
			for r in user_list:
				print "%-12s\t%s" % (r[0],r[1])
	else:
		parser.error("operation %s not recognised" % operation)
