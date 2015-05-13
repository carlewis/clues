#/bin/bash
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

COMANDO_IPMI=/usr/sbin/ipmipower
CAT=cat
AWK=awk
SED=sed
HEAD=head
# User and password needed to connect with the IPMI interfaces
USER=root
PASSWORD=calvin
. /opt/clues/local_config.sh
# Arguments needed by the ipmipower command.
IPMI_ARGS="-u $USER -p $PASSWORD"

function uso() {
	echo "usage: $0 --op|-o [on|off|stat|soft] [ --file|-f <machine file> ] [ machine1 ] [ machine2 ] ... [ machineN ]"
	[ $# -gt 0 ] && exit $1
	exit 0
}
function error() {
	[ $# -eq 0 ] && echo "[ERROR]" 1>&2 && exit 0
	[ $# -eq 1 ] && echo "[ERROR] $1" 1>&2 && exit 0
	echo "[ERROR] $1" 1>&2
	exit $2
}

function info() {
	[ "$QUIET" == "" ] && echo "[INFO] $1"
}

function remove_comments() {
	$CAT $1 | $SED 's/\#.*$//g;/^[ \t]*$/d;s/^[ \t]*//g;s/[ \t]*$//g'
}

function extrae_ip() {
	remove_comments $1 | grep "$2" | $HEAD -n 1 | $AWK '{ print $1 }'
}
function ensurenumber() {
        PARSED=$(echo $1 | $SED 's/[^0-9]//g')
        [ "$PARSED" != "$1" ] && error "$1 is not a number"
}

function doipmi() {
	FICHERO=$1
	MAQUINAS=$2
	OP=$3
	ERR=0
	if [ "$MAQUINAS" == "" ]; then
		for i in $(remove_comments $FICHERO | $AWK '{print $1}')
		do
			info "ipmipower $OP to $i"
			echo "" | $COMANDO_IPMI $IPMI_ARGS -h $i $OP
			exit_code=$?
			if [ $exit_code -ne 0 ]
			then
				error "Error executing the ipmipower command. Exit status $exit_code" 6
				ERR=1
			else
				sleep ${SLP}s
			fi
		done
	else
		for i in $MAQUINAS
		do
			CUR_IP=$(extrae_ip $FICHERO $i)
			if [ "$CUR_IP" != "" ]; then
				info "ipmipower $OP to $CUR_IP"
				echo "" | $COMANDO_IPMI $IPMI_ARGS -h $CUR_IP $OP
				exit_code=$?
				if [ $exit_code -ne 0 ]
				then
					error "Error executing the ipmipower command. Exit status $exit_code" 6
					ERR=1
				else
					sleep ${SLP}s
				fi
			else
				info "cannot find the IP address from $i"
				ERR=1
			fi
		done
	fi
	return $ERR
}

[ ! -x "$COMANDO_IPMI" ] && COMANDO_IPMI=$(which ipmipower)
[ ! -x "$COMANDO_IPMI" ] && error "ipmipower command not found" 1

FICHERO=
MAQUINAS=
SLP=0
QUIET=
while [ $# -gt 0 ]; do
	case "$1" in
		--op|-o)
                        [ $# -lt 2 ] && uso 2
			shift
			OPERATION=$1;;
                --sleep|-s)
                        [ $# -lt 2 ] && uso 2
                        shift
                        SLP=$1;;
		--quiet)
			QUIET=1;;
		--file|-f) 
			[ $# -lt 2 ] && uso 2
			shift
			[ "$MODE" == "" ] && MODE=COMMAND
			FICHERO=$1;;
		--help|-h)
			uso 0;;
		*)
			MAQUINAS="$MAQUINAS $1";;
	esac
	shift
done

ensurenumber $SLP
if [ "$OPERATION" == "" ]
then
	error "operation not provided" 5
else
	if [ "$OPERATION" != "on" -a "$OPERATION" != "off" -a "$OPERATION" != "stat" -a "$OPERATION" != "soft" ]
	then
		error "incorrect operation (on|off|stat|soft)" 6
	else
		OPERATION="--$OPERATION"
	fi
fi
[ "$FICHERO" == "" ] && error "machine file not provided" 3
[ ! -f $FICHERO ] && error "file $FICHERO does not exist" 4
[ "$MAQUINAS" == "" ] && info "no specific machine stated... will use ipmipower to any machine in the file"
doipmi "$FICHERO" "$MAQUINAS" "$OPERATION"
[ $? != 0 ] && exit 1
exit 0
