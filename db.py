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

import time

try:
        import sqlite3 as sqlite
        sqlite3_available=True
        sqlite_available=True
except:
	sqlite3_available=False
	sqlite_available=False

if not sqlite_available:
	try:
		import sqlite
		sqlite_available=True
	except:
		sqlite_available=False

# Class to manage de DB operations
class DataBase:
    
    db_available = sqlite_available
    RETRY_SLEEP = 2
    MAX_RETRIES = 15
    DB_TYPE = "SQLite"
    
    def __init__(self, db_filename):
        self.db_filename = db_filename
        self.connection = None
        
    def connect(self):
        if sqlite_available:
            self.connection = sqlite.connect(self.db_filename)
            return True
        else:
            return False
    
    def _execute_retry(self, sql, args, fetch = False):
        if self.connection is None:
            raise Exception("DataBase object not connected")
        else:
            retries_cont = 0
            while retries_cont < self.MAX_RETRIES:
                try:
                    cursor = self.connection.cursor()
                    if args is not None:
                        if not sqlite3_available:
                            new_sql = sql.replace("?","%s")
                        else:
                            new_sql = sql
                        cursor.execute(new_sql, args)
                    else:
                        cursor.execute(sql)
                    
                    if fetch:
                        res = cursor.fetchall()
                    else:
                        self.connection.commit()
                        res = True
                    return res
                # If the operational error is db lock, retry
                except sqlite.OperationalError, ex:
                    if str(ex).lower() == 'database is locked':
                        retries_cont += 1
                        # release the connection
                        self.close()
                        time.sleep(self.RETRY_SLEEP)
                        # and get it again
                        self.connect()
                    else:
                        raise ex
                except sqlite.IntegrityError, ex:
                    raise IntegrityError()
    
    def execute(self, sql, args = None):
        return self._execute_retry(sql, args)
    
    def select(self, sql, args = None):
        return self._execute_retry(sql, args, fetch = True) 
    
    def close(self):
        if self.connection is None:
            return False
        else:
            try:
                self.connection.close()
                return True
            except Exception, ex:
                return False
            
    def table_exists(self, table_name):
        res = self.select('select name from sqlite_master where type="table" and name="' + table_name + '"')
        if (len(res) == 0):
            return False
        else:
            return True

try:
    class IntegrityError(sqlite.IntegrityError):
        pass
except:
    class IntegrityError:
        pass