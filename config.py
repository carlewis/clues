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

# The version tag
CLUES_VERSION_TAG="0.89"

# This is the directory in which CLUES is installed
CLUES_PATH = "/usr/local/clues"

# Directory in which the plugins are installed
PLUGIN_DIR = CLUES_PATH + "/plugins"

# Directory in which the binaries are installed
BIN_DIR = CLUES_PATH + "/bin"

# Directory in which the hooks are stored
HOOK_DIR = CLUES_PATH + "/bin/hooks"

# Log file
LOGFILE = CLUES_PATH + "/log/clues.log"

# Name of the persistent database (reports and disabled nodes)
CLUES_DB = CLUES_PATH + "/clues.db"

SERVER_HOST= 'localhost'
SERVER_PORT= 8000
#SERVER_PORT= 8001
SERVER_ADDRESS = 'http://%s:%s' %(SERVER_HOST,SERVER_PORT)

# Enables or disables security
SECURITY_TOKEN_ENABLED = True

# How long the authentication tokens are valid (expressed in seconds)
AUTH_TOKEN_EXPIRE = 600

# TIME_UPDATE_STATUS: Time between successive node status updates
TIME_UPDATE_STATUS = 30

# Time a node has to be idle to be switched off
MAX_TIME_IDLE = 7200 # 2 Hours

# Max. time since a poweroff command is executed in a node until the node is
# completely switched off (and ready to be turned on again).
MAX_TIME_POWOFF = 50

# File containing a list of nodes that should be excluded from clues control
EXCLUDED_NODES_FILE=None

# Nodes will be switched on in groups of this size
SWITCH_ON_BLOCKSIZE = 2

# Max. time to wait for a node to boot and be available for job submission
MAX_TIME_BOOT = 300 # 5 mins

# Max. time to wait for the nodes of a job to be available
MAX_WAIT_JOB = 300

# Max. time for the resource manager to launch a job, if there are available
# nodes for the job
MAX_TIME_LAUNCH = 1

# Makes the new_job function async, enabling the job interceptors not to wait
# in case of switching on a node
NEW_JOB_CALL_ASYNC = False

# Number of nodes that must be always available
NODES_AVAILABLE = {}
# If this feature is not necessary set to 0 or comment this line
NODES_AVAILABLE['pbs'] = 2

# Resource managers initially active
MANAGERS=['pbs']

# Power consumption information
# Power consumption of the system when all the nodes are switched off
# It includes the power consumption of the front-end node, the network switches, the KVMs, etc.
MIN_POW = 2012

# Currency of the energy cost
CURRENCY='euro'
# Cost of the energy in CURRENCY per kWh
ENERGY_COST = 0.091

# Group of nodes, if there is only one group the list of nodes is not necessary
# POWER_INFO = {}
# POWER_INFO['g1'] = {}
# POWER_INFO['g1']['NODE_OFF_POW'] = 0
# POWER_INFO['g1']['NODE_USED_POW'] = 0
# POWER_INFO['g1']['NODE_IDLE_POW'] = 0
# POWER_INFO['g1']['nodes'] = ['node1', 'node2', 'node3']

POWER_INFO = {}
POWER_INFO['all'] = {}
POWER_INFO['all']['NODE_OFF_POW'] = 3
POWER_INFO['all']['NODE_USED_POW'] = 205.4
POWER_INFO['all']['NODE_IDLE_POW'] = 130.88


# Hooks
HOOKS={}
# HOOKing system:
# 
# applications to be executed before or after an action done by the clues daemon
#
# Hook Name                  - Parameters  - When is executed
# ======================================================================================================
# HOOK_POWERON               - <hostname>  - Before executing the "boot node" procedure
# HOOK_POWEREDON             - <hostname>  - Once the monitoring system has detected that the node has booted
# HOOK_POWEREDON_UNEXPECTED  - <hostname>  - Once the monitoring system has detected that the node has booted but CLUES has not tried to boot it
# HOOK_POWEROFF              - <hostname>  - Before executing the "boot node" procedure
# HOOK_POWEREDOFF            - <hostname>  - Once the monitoring system has detected that the node has booted
# HOOK_POWEREDOFF_UNEXPECTED - <hostname>  - Once the monitoring system has detected that the node has booted but CLUES has not tried to power it off
# HOOK_MONITORING            - <lrms name> - Before executing the monitoring procedure for <lrms name> lrms
# HOOK_MONITORED             - <lrms name> - After executing the monitoring procedure for <lrms name> lrms
# HOOK_ENABLED               - <hostname>  - Once the system has successfully enabled the node
# HOOK_DISABLED              - <hostname>  - Once the system has successfully disabled the node
#
# The hooks can be defined in two different ways:
#
# To run the hook sinchronously (CLUES daemon will be stopped until the command returns):
#
#   HOOKS['<Hook Name>'] = '<path to the executable, relative to $CLUES/bin/hooks>'
#
# examples
#   HOOKS['HOOK_ENABLED']="enable.hook"
#   HOOKS['HOOK_DISABLED']="disable.hook"
#
# To run the hook asinchronously (CLUES daemon will NO wait the command to return):
#
#   HOOKS['<Hook Name>'] = ('<path to the executable, relative to $CLUES/bin/hooks>', True)
#
# examples
#   HOOKS['HOOK_ENABLED']=("enable.hook", True)
#   HOOKS['HOOK_DISABLED']=("disable.hook", True)
HOOKS['HOOK_POWERON']=None
HOOKS['HOOK_POWEREDON']=None
HOOKS['HOOK_POWEREDON_UNEXPECTED']=None
HOOKS['HOOK_POWEROFF']=None
HOOKS['HOOK_POWEREDOFF']=None
HOOKS['HOOK_POWEREDOFF_UNEXPECTED']=None
HOOKS['HOOK_MONITORING']=None
HOOKS['HOOK_MONITORED']=None
HOOKS['HOOK_ENABLED']=None
HOOKS['HOOK_DISABLED']=None
HOOKS['HOOK_FAIL']=None

# The time that a job must be queued to be evaluated to swithch on new nodes (in secs)
# Use 0 or a negative value to disable re-evaluation
TIME_TO_EVALUATE = 1800
