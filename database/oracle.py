# encoding: utf-8
'''
@project: __oswatch__
@modules: database.oracle
@description:
    
@created:Sep 22, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

import datetime
import os
"""Import module cx_Oracle to connect oracle using python"""
try:
    import cx_Oracle
except ImportError:
    cx_oracle_exists = False
else:
    cx_oracle_exists = True
    
"""Import customized modules"""
from oswatch import logwrite
"""
Module dbocnfig in the package of config to configure 
all information for this project
"""
from config import dbconfig 


class Oracle:      
    def __init__(self, *args):
        '''
        username: oracle user,
        password: oracle user's password,
        mod:  "normal, sysdba, sysoper",defaut is normal,
        host: the oracle database locates
        port: the port listen oracle database service, default is 1521
        insrance: the service name of the oracle database instance 
        '''
        self.username = dbconfig.oracle['username']
        self.password = dbconfig.oracle['password']
        self.mode     = dbconfig.oracle['mode']
        self.host     = dbconfig.oracle['host']
        self.port     = dbconfig.oracle['port']
        self.instance = dbconfig.oracle['instance']

    def __connect(self):
        '''
        connect method
        '''
        
        '''Set lang environment'''
        NLS_LANG = dbconfig.oracle['NLS_LANG']
        os.environ['NLS_LANG'] = NLS_LANG

        if not cx_oracle_exists:
            msg = '''The cx_Oracle module is required. 'pip install cx_Oracle' \
                should do the trick. If cx_Oracle is installed, make sure ORACLE_HOME \
                & LD_LIBRARY_PATH is set'''
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
    
        dsn = cx_Oracle.makedsn(host=self.host, port=self.port, service_name=self.instance)
        try:
            if self.mode == 'sysdba' or self.username == 'sys':
                self.connection = cx_Oracle.connect(self.username, self.password, dsn, \
                                                    mode=cx_Oracle.SYSDBA)
            else:
                self.connection = cx_Oracle.connect(self.username, self.password, dsn)
        except cx_Oracle.DatabaseError as cx_msg:
            msg = 'Could not connect to database: %s, dsn: %s ' % (cx_msg, dsn)
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
        else:
            logwrite.LogWrite(logmessage="Connect oracle "+ self.connection.version+ \
                              " successfully!", loglevel='infoLogger').write_log()
        return self.connection
    
    def __disconnect(self):
        try:
            self.cursor.close()
            self.connection.close()
            logwrite.LogWrite(logmessage="Disconnect from oracle ", \
                              loglevel='infoLogger').write_log()
        except cx_Oracle.DatabaseError as cx_msg:
            msg = cx_msg
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
        
    def select(self, sql, bindvars=''):
        """
        Given a valid SELECT statement, return the results from the database
        """

        results = None

        try:
            self.__connect()
            self.cursor = self.connection.cursor()
            self.cursor.execute(sql, bindvars)
            results = self.cursor.fetchall()
        except cx_Oracle.DatabaseError as cx_msg:
            msg = cx_msg
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
        finally:
            self.__disconnect()
        return results 
      
    def execute(self, sql, bindvars='', many=False, commit=False):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        bindvars is a dictionary of variables you pass to execute.
        """
#         if ('insert' or 'delete' or 'update' in sql.lower()) or commit:
#             commit = True
#         else:
#             commit = False         
        try:
            self.connect()
            self.cursor = self.connection.cursor()
            self.cursor.execute(sql, bindvars)
        except cx_Oracle.DatabaseError as cx_msg:
            msg = cx_msg
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
        else:
            msg = '"'+sql+', '+str(bindvars)+'"'+" completed successfully!"
            logwrite.LogWrite(logmessage=msg, loglevel='infoLogger').write_log()
        finally:
            # Only commit if it-s necessary.
            if commit:
                self.connection.commit()
            else:
                pass
            self.disconnect()
            
class User(object):
    """docstring for Users"""
    
    def __init__(self):
        None
        
    def select(self):
        sql = """select * from all_users  where username like :username"""
        return self.query_sql(sql, {'username':'GZGS%'})
    
    def create(self, username, password, default_tablespace=None, temporary_tablespace=None):
        sql = """CREATE USER {0} IDENTIFIED BY {1}"""

        # Create default tablespace if needs when create user, and default tablespace is user if no tablespace to be assigned
        if default_tablespace is not None:
            sql = sql+" DEFAULT TABLESPACE "+default_tablespace

        # Create temporary tablespace if needs when create user, and default temporary tablespace is temp if no tablespace to be assigned
        if temporary_tablespace is not None:
            sql = sql+" TEMPORARY TABLESPACE "+temporary_tablespace

        # Excute sql to create user
        oracle.Oracle().execute(sql.format(username, password))
        
        logwrite.LogWrite(loglevel='infoLogger', logmessage=sql).write_log()
     
    def drop(self):
        sql = """DROP USER {0} cascade"""
        # Excute sql to drop user (parameter 'cascade' will drop it's objects when drop user)
        oracle.Oracle().execute(sql)
        
        logwrite.LogWrite(loglevel='infoLogger', logmessage=sql).write_log()
     
    def alter(self):
        pass
     
         
class Table(object):
    """docstring for Tables"""
    # def __init__(self, arg):
    #     super(Tables, self).__init__()
    #     self.arg = arg
    def field(self, tablename, owner):
        # Query all columns of the table
        sql = '''select column_name from all_tab_columns \
            where table_name = :1 and owner = :2'''
        results = Oracle().select(sql, (tablename, owner))
        #results = [row[0]  for row in results]
        return results
        
    def primarykey(self, tablename, owner):
        # Query primary key of the table
        sql = '''select col.column_name  from all_constraints con, \
            all_cons_columns col where con.constraint_name = col.constraint_name \
            and con.constraint_type='P' and col.table_name = :1 \
            and col.owner = :2 and con.owner=col.owner'''
        results = Oracle().select(sql, (tablename, owner))
        return results
    
    def tables(self, sql, bindvars=''):
        results = Oracle().select(sql, bindvars)
        return results
        
    def select(self, sql, bindvars=''):
        results = Oracle().select(sql, bindvars)
        return results
    
    def update(self, sql, bindvars='', commit=True):
        helpdocs = """Usage: UPDATE {tablename} SET {field_name}={new value} WHERE {key field}={value}"""
        Oracle().execute(sql, bindvars)
        
    def delete(self, sql, bindvars='', commit=True):
        helpdocs = """Usage: DELETE FROM {tablename} WHERE {key field}={value}"""
        Oracle().execute(sql, bindvars)
    
    def insert(self, sql, bindvars='', commit=True):
        helpdocs = """Usage: INSERT INTO {tablename} VALUES {(fields...)}"""
        Oracle().execute(sql, bindvars)
    
    def create(self):
        helpdocs = """Usage: CREATE TABLE {tablename}"""
        pass
     
    def drop(self):
        helpdocs = """Usage: DROP TABLE {tablename}"""
        pass
     
    def alter(self):
        helpdocs = """Usage: ALTER TABLE {tablename}"""
        pass
     
     
class Tablespace(object):
    """docstring for Tablespace"""
 
    def select(self):
        """ Get tablespace usage
            Monitor  details of the use of tablespace 
        """
        sql = '''SELECT a.tablespace_name, \
            a.bytes/(1024*1024) total_mb, \
            b.bytes/(1024*1024) used_mb, \
            c.bytes/1024/1024 free_mb, \
            round((b.bytes * 100) / a.bytes,2) used_pct, \
            round((c.bytes * 100) / a.bytes,2) free_pct \
            FROM sys.sm$ts_avail a, sys.sm$ts_used b, sys.sm$ts_free c \
            WHERE a.tablespace_name = b.tablespace_name \
            AND a.tablespace_name = c.tablespace_name \
            ORDER BY a.tablespace_name'''
        results = Oracle().select(sql)
        return results
    
    def create(self):
        pass
 
    def drop(self):
        pass
 
    def alter(self):
        pass

class Datafile(object):
    """docstring for Datafiles"""
    def select(self):
        sql = '''SELECT file_id,file_name,online_status,tablespace_name, \
            ROUND(bytes / (1024 * 1024), 2) total_space_mb,autoextensible \
            FROM dba_data_files'''
        results = Oracle().select(sql)
        return results
    
    def create(self):
        pass
 
    def drop(self):
        pass
         
    def alter(self):
        pass
 
class Onlinelog(object):
    """docstring for Onlinelogs"""
    def select(self):
        sql = '''select * from v$logfile'''
        results = Oracle().select(sql)
        return results
    
    def create(self):
        pass
 
    def drop(self):
        pass
 
    def alter(self):
        pass
         
class Archivelog(object):
    """docstring for Archivelogs"""
    def select(self):
        pass
 
    def create(self):
        pass
 
    def drop(self):
        pass
 
    def alter(self):
        pass
 
class Session(object):
    """docstring for Sessions"""
    def select(self):
        '''DocStrins: gv$session and v$session both are views in the oracle, 
        but gv$session shows global rac sessions,including all instance '''
        sql = ''''select count(*) from gv$session where username is not null \
            and status='ACTIVE'''
        params = {'status':'ACTIVE'}
        # return [(row[1],row[2]) for row in self.query_sql(sql,params)]
        return {
            'session_counts':len(self.query_sql(sql, params)),
            'session_sid_serial':[(row[1], row[2]) for row in self.query_sql(sql, params)]
        }
 
    def close(self):
        pass
 
         
 
class Lock(object):
    """docstring for Locks"""    
    def select(self):
        pass
 
    def close(self):
        pass

class OracleBackup(object):
    script_path = dbconfig.script['scriptpath']
    
    from_users = dbconfig.backup['from_users']
    from_tables = dbconfig.backup['from_tables']
    from_tablespaces = dbconfig.backup['from_tables']

    to_users = dbconfig.backup['to_users']
    to_tables = dbconfig.backup['to_tables']
    to_tablespaces = dbconfig.backup['to_tablespaces']

    def exp(self, backup_type=dbconfig.backup['exp']['backup_type'], \
            backup_parameter=dbconfig.backup['exp']['backup_parameter'], \
            backup_path=dbconfig.backup['exp']['backup_path'], \
            backup_user=dbconfig.backup['exp']['backup_user']):

        loglevel = 'infoLogger'
        
        if backup_type.lower() == 'byuser':
            for i in self.from_users:
                text = 'exp {0} owner={1} file={2} log={3} {4}'.format(\
                    backup_user, i, backup_path+'/'+i+'_exp_imp_byuser_'+ \
                    datetime.datetime.now().strftime('%Y%m%d') + '.dmp',backup_path + '/' + \
                    i+'_exp_byuser_'+datetime.datetime.now().strftime('%Y%m%d')+'.log', \
                    backup_parameter)   
        elif backup_type.lower() == 'bytable':
            for i in self.from_users:
                for k in self.from_tables:
                    text = "exp {0} table={1} file={2} log={3} {4}".format(\
                        backup_user, i + '.' + k, backup_path + '/' + i + \
                        '.' + k + '_exp_imp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + \
                        '.dmp',backup_path + '/' + i + '.' + k + '_exp_bytable_' + \
                        datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
        elif backup_type.lower() == 'byfull':
            text = "exp {0} full=y file={1} log={2} {3}".format(backup_user,
                backup_path + '/' + dbconfig.oracle['instance'] + '_exp_imp_byfull_' + 
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                backup_path + '/' + dbconfig.oracle['instance'] + '_exp_byfull_' + 
                datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)   
        else:
            text = "Please asign the type of backup. Such as byuser|bytable|byfull."
            loglevel = 'warnLogger'
       
        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()
                
    def expdp(self, backup_type=dbconfig.backup['expdp']['backup_type'], \
            backup_parameter=dbconfig.backup['expdp']['backup_parameter'], \
            backup_path=dbconfig.backup['expdp']['backup_path'], \
            backup_user=dbconfig.backup['expdp']['backup_user']):
        
        loglevel = 'infoLogger'
        
        if backup_type.lower() == 'byuser':
            for i in self.from_users:
                text = "expdp {0} schemas={1} directory={2} dumpfile={3} logfile={4} {5}".format(\
                    backup_user, i, backup_path, i + '_expdp_impdp_byuser_' + \
                    datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                    i + '_expdp_byuser_' + datetime.datetime.now().strftime('%Y%m%d') + '.log', \
                    backup_parameter)
        elif backup_type.lower() == 'bytable':
            for i in self.from_users:
                for k in self.from_tables:
                    text = "expdp {0} tables={1} directory={2} dumpfile={3} logfile={4} {5}".format(\
                        backup_user, i + '.' + k, backup_path, i + '.' + k + '_expdp_impdp_bytable_' + \
                        datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                        i + '.' + k + '_expdp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + '.log', \
                        backup_parameter)
        elif backup_type.lower() == 'bytablespace':
            for i in self.from_tablespaces:
                text = "expdp {0} tablespaces={1} directory={2} dumpfile={3} logfile={4} {5}".format(\
                    backup_user, i, backup_path, i + '_expdp_impdp_bytablespace_' + \
                    datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                    i + '_expdp_bytablespace_' + datetime.datetime.now().strftime('%Y%m%d') + '.log', \
                    backup_parameter)
        elif backup_type.lower() == 'byfull':
            text = "expdp {0} full=y directory={1} dumpfile={2} logfile={3} {4}".format(\
                backup_user, backup_path, dbconfig.oracle['instance'] + '_expdp_impdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                dbconfig.oracle['instance'] + '_expdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
        else:
            text = "Please asign the type to backup data.Such as byuser|bytable|bytablespace|byfull."   
            loglevel = 'warnLogger'
        
        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()
        
class OracleRecovery(object):
    script_path = dbconfig.script['scriptpath']
    
    from_users = dbconfig.backup['from_users']
    from_tables = dbconfig.backup['from_tables']
    from_tablespaces = dbconfig.backup['from_tables']

    to_users = dbconfig.backup['to_users']
    to_tables = dbconfig.backup['to_tables']
    to_tablespaces = dbconfig.backup['to_tablespaces']
    
    def imp(self, restore_type=dbconfig.backup['imp']['restore_type'], \
            restore_parameter=dbconfig.backup['imp']['restore_parameter'], \
            restore_path=dbconfig.backup['imp']['restore_path'], \
            restore_user=dbconfig.backup['imp']['restore_user']):
        
        loglevel = 'infoLogger'

        if restore_type.lower() == 'byuser':
            for (i, j) in zip(self.from_users, self.to_users):
                text = "imp {0} fromuser={1} touser={2} file={3} log={4} {5}".format(\
                    restore_user, i, j, restore_path + '/' + i + '_exp_imp_byuser_' + \
                    datetime.datetime.now().strftime('%Y%m%d') + '.dmp',\
                    restore_path + '/' + i + '_imp_byuser_' + \
                    datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
        elif restore_type.lower() == 'bytable':
            for (i, j) in zip(self.from_users, self.to_users):
                for k in self.from_tables:
                    text = "imp {0} fromuser={1} touser={2} table={3} file={4} log={5} {6}".format(\
                        restore_user, i, j, k, restore_path + '/' + i + '.' + k + '_exp_imp_bytable_' + \
                        datetime.datetime.now().strftime('%Y%m%d') + '.dmp',\
                        restore_path + '/' + i + '.' + k + '_imp_bytable_' + \
                        datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
        elif restore_type.lower() == 'byfull':
            text = "imp {0} full=y file={1} log={2} ignore=y".format(\
                restore_user,restore_path + '/' + dbconfig.oracle['instance'] + '_exp_imp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',restore_path + '/' + \
                dbconfig.oracle['instance'] + '_imp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log')
        else:
            text = "Please asign the type of import data.Like byuser|bytable|byfull."
            loglevel = 'warnLogger'
                
        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()

    def impdp(self, restore_type=dbconfig.backup['impdp']['restore_type'], \
            restore_parameter=dbconfig.backup['impdp']['restore_parameter'], \
            restore_path=dbconfig.backup['impdp']['restore_path'], \
            restore_user=dbconfig.backup['impdp']['restore_user']):

        loglevel = 'infoLogger'

        if restore_type.lower() == 'byuser':
            for (i, j) in zip(self.from_users, self.to_users):
                text = "impdp {0} remap_schemma={1} directory={2} dumpfile={3} logfile={4}  {5}".format(\
                    restore_user, i + ':' + j, restore_path, i + '_expdp_impdp_byuser_' + \
                    datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',i + '_impdp_byuser_' + \
                    datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
        elif restore_type.lower() == 'bytable':
            for (i , j) in zip(self.from_users, self.to_users): 
                for (k, m) in zip(self.from_tables, self.to_tables):
                    text = "impdp {0} tables={1} remap_schemma={2} directory={3} dumpfile={4} logfile={5}  {6}".format(\
                        restore_user, k, i + ':' + j, restore_path, i + '.' + k + '_expdp_impdp_bytable_' + \
                        datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',i + '.' + k + \
                        '_impdp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
        elif restore_type.lower() == 'bytablespace':
            for (i , j) in zip(self.from_users, self.to_users): 
                for (k, m) in zip(self.from_tablespaces, self.to_tablespaces):
                    text = "impdp {0} remap_schemma={1} remap_tablespace={2} directory={3} dumpfile={4} logfile={5}  {6}".format(\
                        restore_user, i + ':' + j, k + ':' + m, restore_path, k + '_expdp_impdp_bytablespace_' + \
                        datetime.datetime.now().strftime('%Y%m%d') + '.dmp',k + '_impdp_bytablespace_' + \
                        datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
        elif restore_type.lower() == 'byfull':
            text = "impdp {0} full=y directory={1} dumpfile={2} logfile={3}  {4}".format(\
                restore_user, restore_path, dbconfig.oracle['instance'] + \
                '_expdp_impdp_byfull_' + datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                dbconfig.oracle['instance'] + '_impdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
        else:
            text = "Please asign the type to import data.Like byuser|bytable|bytablespace|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()


if __name__ == '__main__':
    #print(OracleBackup().expdp(backup_type='bytablespace'))
    if Table().fieldname('A_QYMC', 'GZGS_HZ') == Table().fieldname('A_QYMC', 'GZGS_GY'):
        print("yes")
    else:
        print("no")
        
    
    