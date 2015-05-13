#!/bin/python
#
# CLUES ONE Connector - ONE Connector for Cluster Energy Saving System
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
import re
import logging

class ReqsEvaluation:
	def __init__(self):
		self.__vars = { }
		self.__symbols = { '=' : '==', '&' : 'and', '|' : 'or' }
		self.__symbols_correct = { '>==' : '>=', '<==' : '<=' }

	def add_defaults(self):
		self.add_var('TRUE',1)
		self.add_var('True',1)
		self.add_var('true',1)
		self.add_var('FALSE',0)
		self.add_var('False',0)
		self.add_var('false',0)

	def add_var(self, var, value):
		if (not isinstance(var, str) or re.search("^[a-zA-Z_]", var) is None):
			return False
		
		self.__vars[var] = value
		return True

	def add_var_detect_numeric(self, var, value):
		if re.match("^\"[^\"]*\"$", str(value)) is not None:
			# Estamos poniendo algo entrecomillado de forma expresa, asi que se ha de interpretar como una cadena
			value_mod = str(value)
		else:
			try:
				# Si lo podemos pasar a float es que es numerico
				value_mod = float(value)
			except:
				# No lo hemos podido pasar a float, asi que es una cadena
				value_mod = value

		return self.add_var(var, value_mod)

	def __perform_substitutions(self, txt):
		for var, value in self.__vars.items():
			if (isinstance(value, int) or isinstance(value, float)):
				repl_val = str(value)
			else:
				if re.match("^\"[^\"]*\"$", str(value)) is not None:
					# si ya es una cadena entrecomillada, la aceptaremos como valida
					repl_val = str(value)
				else:
					# y si no, la entrecomillamos de forma obligatoria
					repl_val = "\"" + str(value).replace("\"", "\\\"") + "\""

			txt = re.sub(r'\b' + var + r'\b', repl_val, txt)
		return txt

	def __replace_symbols(self, txt):
		for var, value in self.__symbols.items():
			txt = txt.replace(var, value)
		for var, value in self.__symbols_correct.items():
			txt = txt.replace(var, value)

		return txt

	def __is_secure(self, txt):
		# sustituimos cualquier cadena entrecomillada que haya por un numero
		numeric_str = re.sub("\"[^\"]*\"", "0", txt)
		
		# ahora miramos a ver si todo son numeros o expresiones
		valid = re.search("[^.0-9+-/*()&\|><=!\s]", numeric_str) is None

		# print numeric_str, txt, valid
		return valid

	def __str__(self):
		return str(self.__vars)

	def eval(self, txt = ""):
		# The expression must be a text
		txt = str(txt)
		if txt == "":
			# An exception to consider the default behavior
			return True
		subs_str = self.__perform_substitutions(txt)
		if (self.__is_secure(subs_str)):
			subs_str = self.__replace_symbols(subs_str)
			try:
				result = eval(subs_str)
			except:
				raise Exception("not a well formed expression")

			try:
				result = int(result)
				result = (result != 0)
			except:
				pass
			return result
		else:
			raise Exception("not a valid expression (%s)" % subs_str)

if __name__ == "__main__":
        reqs = ReqsEvaluation()
        
        reqs.add_var('UNO',1)
        reqs.add_var('HYPERVISOR','\"kvm\"')
        reqs.add_var('DOS',2)
	reqs.add_var('TRUE',1)
	reqs.add_var('FALSE',0)
        reqs.add_var(1,1)
        reqs.add_var(reqs,"f")
	reqs.add_var_detect_numeric('USEDMEMORY','449856')

        print reqs.eval("HYPERVISOR = \"kvm\" & 1*5>4")
	print reqs.eval("USEDMEMORY>=449857")
        print reqs.eval("TRUE")
        print reqs.eval("TRUE & FALSE")
	print reqs.eval("")
