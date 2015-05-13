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

COMANDO_WOL=/sbin/ether-wake
PARAMS_WOL=""
# In some ether-wake versions the interface must be specified
#PARAMS_WOL="-i eth0"
CAT=cat
AWK=awk
SED=sed
HEAD=head

function uso() {
	echo "usage: $0 [ --file|-f <machine file> ] [ machine1 ] [ machine2 ] ... [ machineN ]"
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

function extrae_mac() {
	remove_comments $1 | grep "$2" | $HEAD -n 1 | $AWK '{ print $2 }'
}
function ensurenumber() {
        PARSED=$(echo $1 | $SED 's/[^0-9]//g')
        [ "$PARSED" != "$1" ] && error "$1 is not a number"
}

function dowol() {
	FICHERO=$1
	MAQUINAS=$2
	ERR=0
	if [ "$MAQUINAS" == "" ]; then
		for i in $(remove_comments $FICHERO | $AWK '{print $2}')
		do
			info "wol to $i"
			$COMANDO_WOL $PARAMS_WOL $i && sleep ${SLP}s
		done
	else
		for i in $MAQUINAS
		do
			CUR_MAC=$(extrae_mac $FICHERO $i)
			if [ "$CUR_MAC" != "" ]; then
				info "wol to $CUR_MAC"
				$COMANDO_WOL $PARAMS_WOL $CUR_MAC
				echo $COMANDO_WOL $PARAMS_WOL $CUR_MAC
				sleep ${SLP}s
			else
				info "cannot find the mac address from $i"
				ERR=1
			fi
		done
	fi
	return $ERR
}

[ ! -x "$COMANDO_WOL" ] && COMANDO_WOL=$(which etherwake)
[ ! -x "$COMANDO_WOL" ] && COMANDO_WOL=$(which ether-wake)
[ ! -x "$COMANDO_WOL" ] && error "etherwake command not found" 1

FICHERO=
MAQUINAS=
SLP=0
QUIET=
while [ $# -gt 0 ]; do
	case "$1" in
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
[ "$FICHERO" == "" ] && error "machine file not provided" 3
[ ! -f $FICHERO ] && error "file $FICHERO does not exist" 4
[ "$MAQUINAS" == "" ] && info "no specific machine stated... will WOL to any machine in the file"
dowol "$FICHERO" "$MAQUINAS"
[ $? != 0 ] && exit 1
exit 0
