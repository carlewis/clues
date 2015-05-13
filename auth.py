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
#
import hashlib
import os
import logging
import uuid
from db import DataBase

class Auth:
	AUTH_METHOD_TOKEN="token"
	AUTH_METHOD_PLAIN="plain"

	@staticmethod
	def split_token_session(token_session):
		# This method simply splits the session token in its components: method, username and token_id
		token_data = token_session.split(":")
		if len(token_data) != 3:
			return None, None, None
		if token_data[0] not in (Auth.AUTH_METHOD_TOKEN, Auth.AUTH_METHOD_PLAIN):
			return None, None, None
		return token_data

	@staticmethod
	def generate_token():
		# The token is automatically generated and converted to a single number by hashing it (the single token will be valid, indeed)
		token = uuid.uuid4()
		hasher = hashlib.sha1()
		hasher.update(str(token))
		token = hasher.hexdigest()
		return token

	@staticmethod
	def create_session_token(method, username, passwd):
		if method not in (Auth.AUTH_METHOD_TOKEN, Auth.AUTH_METHOD_PLAIN):
			return "::"
		return "%s:%s:%s" % (method, username, passwd)

class TableNotFound(Exception):
	pass
class AuthServer_DB:
	def __init__(self, expiry_time, db_name = "users.db", create_table = False):
		# The database name where the users (and tokens) are stored
		self.__db_name = db_name
		# The expiry time is expressed in seconds and represents how long the authentication token is valid from the creation time
		self.__expiry_time = expiry_time
		# We are trying to validate the users database
		db = DataBase(self.__db_name)
		db.connect()
		if not db.table_exists("user"):
			if create_table:
				# if the table does not exist, we will create it
				sentence = '''create table "user" ('''
				sentence = sentence + ''' "user" TEXT PRIMARY KEY NOT NULL, '''
				sentence = sentence + ''' "password" TEXT NOT NULL, '''
				sentence = sentence + ''' "attributes" TEXT, '''
				sentence = sentence + ''' "token" TEXT, '''
				sentence = sentence + ''' "token_expire" INTEGER '''
				sentence = sentence + ''' ) '''
				db.execute(sentence)
			else:
				raise TableNotFound

	def create_user(self, username, password, attributes = []):
		if username is None:
			return False

		success = False
		try:
			db = DataBase(self.__db_name)
			db.connect()
			attributes_string = ";".join(attributes)
			db.execute("insert into user (user, password, attributes) values (\"%s\", \"%s\", \"%s\");" % (username, password, attributes_string))
			success = True
			logging.debug("user '%s' successfully created" % username)
		except Exception, e:
			logging.error("error trying to create user '%s' (%s)" % (username, str(e)))
		return success

	def delete_user(self, username):
		if username is None:
			return False

		success = False
		try:
			db = DataBase(self.__db_name)
			db.connect()
			db.execute("delete from user where user=\"%s\";" % username)
			success = True
			logging.debug("user '%s' successfully deleted" % username)
		except Exception, e:
			logging.error("error trying to delete user '%s' (%s)" % (username, str(e)))
		return success

	def get_user_list(self):
		success = False
		user_list = []
		try:
			db = DataBase(self.__db_name)
			db.connect()
			user_list = db.select("select user, attributes from user;")
			success = True
			logging.debug("user list successfully obtained")
		except Exception, e:
			logging.error("error trying to list users (%s)" % (str(e)))
		return (success, user_list)

	def get_user_data_from_db(self, username):
		t_user = None
		t_attributes = []
		try:
			db = DataBase(self.__db_name)
			db.connect()
			datos = db.select("select user, attributes from user where user=\"%s\";" % username)
			if (len(datos) != 0):
				t_user, t_attributes = datos[0]
				t_attributes = t_attributes.split(";")
		except Exception, e:
			logging.error("an error happened trying to obtain data from user '%s' (%s)" % (username, str(e)))
		return t_user, t_attributes

	def ensure_attributes(self, token_session, attributes, ensure_all = True):
		t_method, t_username, t_password = Auth.split_token_session(token_session)
		return self.__ensure_attributes(t_username, attributes, ensure_all)

	def __ensure_attributes(self, username, attributes, ensure_all = True):
		try:
			user, user_attributes = self.get_user_data_from_db(username)
		except:
			return False
		if user is None:
			return False

		for g in attributes:
			if g in user_attributes:
				if not ensure_all:
					return True
			else:
				return False
		return True

	def init_session_by_token(self, token_session):		
		t_method, t_username, t_password = Auth.split_token_session(token_session)
		return self.init_session(t_method, t_username, t_password)

	def init_session(self, method, username, password):
		if username is None:
			return False, "token:%s:" % username

		token = None
		if method == Auth.AUTH_METHOD_PLAIN:
			try:
				db = DataBase(self.__db_name)
				db.connect()
				datos = db.select("select user, password, token from user where user=\"%s\";" % username)
				t_user = t_password = t_token = None
				if (len(datos) != 0):
					t_user, t_password, t_token = datos[0]

				if password == t_password:
					token = Auth.generate_token()
					db.execute("update user set token=\"%s\", token_expire=strftime('%%s',datetime('now','+%d seconds')) where user=\"%s\";" % (token, 
							self.__expiry_time, t_user))
					logging.debug("user '%s' succeeded to authenticate using password" % username)
				else:
					logging.debug("user '%s' failed to authenticate using password" % username)
			except Exception, e:
				logging.error("an error happened trying to authenticate user '%s' (%s)" % (username, str(e)))
				token = None

		elif method == Auth.AUTH_METHOD_TOKEN:
			try:
				db = DataBase(self.__db_name)
				db.connect()
				datos = db.select("select user, token, token_expire - strftime('%%s',datetime('now')) from user where user=\"%s\";" % username)
				t_user = t_token = t_token_expire = None
				if (len(datos) != 0):
					t_user, t_token, t_token_expire = datos[0]
	
				if password == t_token and t_token_expire > 0:
					token = t_token
					db.execute("update user set token=\"%s\", token_expire=strftime('%%s',datetime('now','+%d seconds')) where user=\"%s\";" % (t_token, 
							self.__expiry_time, t_user))
					logging.debug("user '%s' succeeded to authenticate using an authentication token" % username)
				else:
					logging.debug("user '%s' failed to authenticate using an authentication token" % username)
			except Exception, e:
				logging.error("an error happened trying to authenticate user '%s' (%s)" % (username, str(e)))
				token = None

		if token is None:
			return False, "token:%s:" % username
		else:
			return True, "token:%s:%s" % (username, token)

class AuthServer_dummy(AuthServer_DB):
	def __init__(self, expiry_time = 0, db_name = None):
		# We do not want to check the database
		pass

	def get_user_data_from_db(self, username):
		# It is impossible to get the data from the user
		return None, []

	def init_session(self, method, username, password):
		# It always is able to init the session
		if username is None:
			return False, "token:%s:" % username

		token = Auth.generate_token()
		return True, "token:%s:%s" % (username, token)

	def create_user(self, username, password, attributes = []):
		# No user can be created
		return False

	def ensure_attributes(self, token_session, attributes, ensure_all = True):
		# Every user has any attribute for not limiting the permissions
		return True

if DataBase.db_available:
	class AuthServer(AuthServer_DB): pass
else:
	class AuthServer(AuthServer_dummy): pass
	#logging.warn("Database " + DataBase.DB_TYPE + " not available, so not using authentication mechanisms")

class AuthClient:
	# This AuthClient class helps in authenticating to an eventual server that will conform to
	# the AuthServer.init_session interface and return values
	# 
	# The AuthServer.init_sessions receives
	# 	(TOKEN_SESSION) 
	# and returns a tuple
	# 	(SUCCESS, TOKEN_SESSION)
	#
	# If the proxy passed (maybe a xmlrpc proxy or any other object) is able to respond in such
	# manner, these functions will carry out the authentication of the user.
	#
	# Otherwise you are able to use some of the static functions here or take this class as a
	# template for your specific authorisation 
	#
	def __init__(self, auth_proxy):
		self.token_session = None
		self.token_id = None
		self.username = None
		self.password = None
		self.__server = auth_proxy

	@staticmethod
	def get_token_session_from_file(token_filename=None, username=None):
		success = False
		token_session = "token:%s:" % username
		if token_filename is not None:
			(success, sess_username, token) = AuthClient.get_id_from_file(token_filename, username)
			if success:
				token_session = Auth.create_session_token(Auth.AUTH_METHOD_TOKEN, username, token)

		if success:
			return True, token_session
		else:
			return False, "token::"

	@staticmethod
	def get_token_pass_from_file(passwd_filename=None, username=None):
		success = False
		token_session = "token:%s:" % username

		if passwd_filename is not None:
			(success, sess_username, passwd) = AuthClient.get_id_from_file(passwd_filename, username)
			if success:
				token_session = Auth.create_session_token(Auth.AUTH_METHOD_PLAIN, sess_username, passwd)

		if success:
			return True, token_session
		else:
			return False, "plain::"

	def init_session_from_files(self, token_filename=None, username=None, passwd_filename=None):
		success = False
		token_session = "token:%s:" % username
		if token_filename is not None:
			(success, sess_username, token) = AuthClient.get_id_from_file(token_filename, username)
			if success:
				# intentamos iniciar sesion remota
				if username == sess_username:
					success, token_session = self.__server.init_session(Auth.create_session_token(Auth.AUTH_METHOD_TOKEN, sess_username, token))
				else:
					success = False

		if not success and passwd_filename is not None:
			(success, sess_username, passwd) = AuthClient.get_id_from_file(passwd_filename, username)
			if success:
				# intentamos iniciar sesion remota
				if username == sess_username:
					success, token_session = self.__server.init_session(Auth.create_session_token(Auth.AUTH_METHOD_PLAIN, sess_username, passwd))
				else:
					success = False
		if success:
			self.username = sess_username
			self.token_session = token_session
			AuthClient.save_token_file(token_filename, token_session)	
			return True, token_session
		else:
			return False, "token::"

	def init_session_by_pass(self, token_filename=None, username=None, password=None):
		success = False
		if username is None:
			return False
		if password is None:
			return False

		hasher = hashlib.sha1()
		hasher.update(password)
		password = hasher.hexdigest()

		success, token_session = self.__server.init_session(Auth.create_session_token(Auth.AUTH_METHOD_PLAIN, username, password))

		if success:
			self.username = username
			self.token_session = token_session
			AuthClient.save_token_file(token_filename, token_session)	
			return True, token_session
		else:
			return False, "token::"

	@staticmethod
	def get_id_from_file(filename, overriding_username):
		auth_file = os.path.expanduser(filename)
		username=None
		password=None
		read_success=False
		if (os.path.isfile(auth_file)):
			try:
				fauth = open(auth_file, "rt")
				auth_str = fauth.readline()
				fauth.close()
				auth_tokens = auth_str.strip().split(":")
				n_tokens = len(auth_tokens)
				if n_tokens == 1:
					username = auth_tokens[0]
				elif n_tokens == 2:
					username = auth_tokens[0]
					password = auth_tokens[1]
				if overriding_username is not None:
					username = overriding_username
				read_success=True
			except:
				read_success=False
	
		return (read_success, username, password)

	@staticmethod
	def save_token_file(auth_token_file, token_session):
		if auth_token_file is None:
			return False

		method, username, token = Auth.split_token_session(token_session)

		auth_token_file = os.path.expanduser(auth_token_file)
		auth_token_dir = os.path.dirname(auth_token_file)
		if not os.path.isdir(auth_token_dir):
			try:
				os.makedirs(auth_token_dir)
				os.chmod(auth_token_dir, 448)		# 448 es 700 en octal
			except:
				logging.warning("could not create the auth directory (%s)" % auth_token_dir)
		try:
			fauth_token = open(auth_token_file, "wt")
			fauth_token.write("%s:%s" % (username, token))
			os.chmod(fauth_token.fileno(), 384)		# 384 es 600 en octal
			fauth_token.close()
			return True
		except:
			logging.warning("could not create the token file")
			return False

	def create_user(self, token_session, username=None, password=None):
		if username is None:
			return False
		hasher = hashlib.sha1()
		hasher.update(password)
		password = hasher.hexdigest()
		#
		# This function assumes the existance of a create_user in the server
		# but it is only included for demonstration purposes on how to use this
		# class. This function should be removed from here and translated into
		# an stand-alone application for maintaining the list of authorized users
		#
		success = self.__server.create_user(token_session, username, password)
