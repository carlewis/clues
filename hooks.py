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

import subprocess
import os
import logging
from clues_daemon import HOOKS, HOOK_DIR, CommandError

# -------------------------------------------------------------------------------------------------
# HOOKS:
# -------------------------------------------------------------------------------------------------

def run_command(command, async=False, shell=False):
   try:
      p=subprocess.Popen(command, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=shell)
   except:
      if type(command)==list: command = "".join(command)
      logging.error('Could not execute command "%s"' %command)
      raise

   # if the execution is async we must not wait for the results
   if async:
      return ""
   else:
      (out, err) = p.communicate()
      if p.returncode!=0:
         if type(command)==list: command = "".join(command)
         logging.error(' Error in command "%s"' % command)
         logging.error(' Return code was: %s' % p.returncode)
         logging.error(' Error output was:\n%s' % err)
         raise CommandError()
      else:
         return out


HOOKS_NAMES = ['HOOK_ENABLED', 'HOOK_DISABLED', 
   'HOOK_POWERON', 'HOOK_POWEREDON', 'HOOK_POWEROFF', 
   'HOOK_POWEREDOFF', 'HOOK_POWEREDON_UNEXPECTED', 
   'HOOK_POWEREDOFF_UNEXPECTED', 'HOOK_MONITORING', 
   'HOOK_MONITORED', 'HOOK_FAIL']

class HookError(Exception):
   """Exception: hook does not exist"""
   def __init__(self, hookname):
      self.hookname = hookname
   def __str__(self):
      return "hook '%s' does not exist" % self.hookname
   
def execute_hook(hook_name, params=[]):
   if hook_name not in HOOKS_NAMES:
      raise HookError(hook_name) 
   try:
      if isinstance(HOOKS[hook_name], tuple):
         (command, async) = HOOKS[hook_name]
      else:
         command = HOOKS[hook_name]
         async = False

      if (command is not None) and (command != ""):
         hook_app = HOOK_DIR + "/" + command
         logging.debug(hook_app)
         if os.path.isfile(hook_app):
            output = run_command([hook_app]+params, async)
            if not async:
               logging.debug("hook '%s' output:\n%s" % (hook_name, output))
         else:
            logging.warning("executable '%s' for hook '%s' is not valid" % (hook_app, hook_name))
   except CommandError:
      logging.error("hook '%s' did not return zero\noutput:\n%s" % (hook_name, output))
      return False
   except Exception, ex:
      logging.error("hook '%s' did not return zero" % (hook_name))
      logging.error(ex)
      return False


