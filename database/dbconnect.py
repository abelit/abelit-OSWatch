# encoding: utf-8
'''
@project: __oswatch__
@modules: database.dbconnect
@description:
    
@created:Aug 2, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

import cx_Oracle  # @UnresolvedImport
import sys
import os

# Import customize modules
from config import dbconfig
from core import logwrite

class DBConnect(object):
    # Connect to oracle
    def conn_oracle(self):
        username = dbconfig.oracle['username']
        password = dbconfig.oracle['password']
        host = dbconfig.oracle['host']
        port = dbconfig.oracle['port']
        instance = dbconfig.oracle['instance']
        NLS_LANG = dbconfig.oracle['NLS_LANG']
        os.environ['NLS_LANG'] = NLS_LANG       
        try:
            if username == 'sys':
                conn = cx_Oracle.connect(username, password, host + ':' + str(port) + '/' + instance, cx_Oracle.SYSDBA)
            else:
                conn = cx_Oracle.connect(username, password, host + ':' + str(port) + '/' + instance)
        except cx_Oracle.DatabaseError as cx_msg:
            logwrite.LogWrite(logmessage="Failed to connect to Database " + str(cx_msg), loglevel='errorLogger').write_log()
            sys.exit()
        else:
            logwrite.LogWrite(logmessage="Connect oracle "+ conn.version+" successfully",loglevel='infoLogger').write_log()
            return conn
                
    # Connect to mysql
    def conn_mysql(self):
        pass

if __name__ == '__main__':
    DBConnect().conn_oracle()