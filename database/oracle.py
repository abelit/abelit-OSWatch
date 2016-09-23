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
'''
Import module cx_Oracle to connect oracle using python
'''
try:
    import cx_Oracle
except ImportError:
    cx_oracle_exists = False
else:
    cx_oracle_exists = True
    
"""Import customized modules"""
from oswatch import texthandler
from oswatch import logwrite
from config import dbconfig # Module dbocnfig in the package of config to configure all information for this project

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

    def connect(self):
        '''
        connect method
        '''
        
        '''Set lang environment'''
        NLS_LANG = dbconfig.oracle['NLS_LANG']
        os.environ['NLS_LANG'] = NLS_LANG

        if not cx_oracle_exists:
            msg = "The cx_Oracle module is required. 'pip install cx_Oracle' should do the trick. If cx_Oracle is installed, make sure ORACLE_HOME & LD_LIBRARY_PATH is set"
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
    
        dsn = cx_Oracle.makedsn(host=self.host, port=self.port, service_name=self.instance)
        try:
            if self.mode == 'sysdba' or self.username == 'sys':
                self.connection = cx_Oracle.connect(self.username, self.password, dsn, mode=cx_Oracle.SYSDBA)
            else:
                self.connection = cx_Oracle.connect(self.username, self.password, dsn)
        except cx_Oracle.DatabaseError as cx_msg:
            msg = 'Could not connect to database: %s, dsn: %s ' % (cx_msg, dsn)
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
        else:
            logwrite.LogWrite(logmessage="Connect oracle "+ self.connection.version+" successfully!", loglevel='infoLogger').write_log()
        return self.connection
    
    def disconnect(self):
        try:
            self.cursor.close()
            self.connection.close()
            logwrite.LogWrite(logmessage="Disconnect from oracle ",loglevel='infoLogger').write_log()
        except cx_Oracle.DatabaseError as cx_msg:
            msg = cx_msg
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
        
    def select(self, sql, bindvars=''):
        """
        Given a valid SELECT statement, return the results from the database
        """

        results = None

        try:
            self.connect()
            self.cursor = self.connection.cursor()
            self.cursor.execute(sql, bindvars)
            results = self.cursor.fetchall()
        except cx_Oracle.DatabaseError as cx_msg:
            msg = cx_msg
            logwrite.LogWrite(logmessage=msg, loglevel='errorLogger').write_log()
        finally:
            self.disconnect()
        return results 
      
    def execute(self, sql, bindvars='', commit=False):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        bindvars is a dictionary of variables you pass to execute.
        """
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
            
class Users(object):
    """docstring for Users"""
    
    def __init__(self):
        None
        
    def query_user(self):
        sql = '''
        select * from all_users  where username like :username
        '''
        return self.query_sql(sql, {'username':'GZGS%'})
    
    def create_user(self):
        pass
     
    def drop_user(self):
        pass
     
    def set_user(self):
        pass
     
         
class Tables(object):
    """docstring for Tables"""
    # def __init__(self, arg):
    #     super(Tables, self).__init__()
    #     self.arg = arg
    def query_column(self, tablename, owner):
        # Query primary key of the table
        sql_pk = '''
        select col.column_name  from all_constraints con,  all_cons_columns col where con.constraint_name = col.constraint_name and con.constraint_type='P' and col.table_name = :table_name and col.owner = :owner and con.owner=col.owner
        '''
        # Query all columns of the table
        sql_nm = '''
        select column_name from all_tab_columns where table_name = :table_name and owner = :owner
        '''
        # Return result of formatting sql text
        return {
            'column_pk':[row[0] for row in self.query_sql(sql_pk, {'table_name':tablename, 'owner':owner})],
            'column_nm':[row[0] for row in self.query_sql(sql_nm, {'table_name':tablename, 'owner':owner})]
        }
     
    def query_table(self):
        pass
         
    def create_table(self):
        pass
     
    def drop_table(self):
        pass
     
    def set_table(self):
        pass
     
     
class Tablespaces(object):
    """docstring for Tablespaces"""
    def __init__(self, arg):
        super(Tablespaces, self).__init__()
        self.arg = arg
 
    def query_tablespace(self):
        pass
         
    def create_tablespace(self):
        pass
 
    def set_tablespace(self):
        pass
 
    def drop_tablespace(self):
        pass
 
class Datafiles(object):
    """docstring for Datafiles"""
    def __init__(self, arg):
        super(Datafiles, self).__init__()
        self.arg = arg
 
    def query_datafile(self):
        pass
 
    def create_datafile(self):
        pass
 
    def set_datafile(self):
        pass
         
    def drop_datafile(self):
        pass
 
class Onlinelogs(object):
    """docstring for Onlinelogs"""
    def __init__(self, arg):
        super(Onlinelogs, self).__init__()
        self.arg = arg
 
    def query_onlinelog(self):
        pass
 
    def add_onlinelog(self):
        pass
 
    def delete_onlinelog(self):
        pass
 
    def set_onlinelog(self):
        pass
         
class Archivelogs(object):
    """docstring for Archivelogs"""
    def __init__(self, arg):
        super(Archivelogs, self).__init__()
        self.arg = arg
         
 
class Sessions(object):
    """docstring for Sessions"""
    def query_session(self):
        sql = '''
            select * from v$session where status=:status
            '''
        params = {'status':'ACTIVE'}
        # return [(row[1],row[2]) for row in self.query_sql(sql,params)]
        return {
            'session_counts':len(self.query_sql(sql, params)),
            'session_sid_serial':[(row[1], row[2]) for row in self.query_sql(sql, params)]
        }
 
    def close_session(self):
        pass
 
         
 
class Locks(object):
    """docstring for Locks"""
    def __init__(self, arg):
        super(Locks, self).__init__()
        self.arg = arg
     
    def query_lock(self):
        pass
 
    def release_lock(self):
        pass
     

class DataBackupRestore(object):
    """docstring for DataBackupRestore"""
    script_path = dbconfig.script['scriptpath']
    
    from_users = ['GZGS_GY']
    from_tables = ['A_BM_XZQH']
    from_tablespaces = ['GZGS_GY']

    to_users = ['GZGS_GY']
    to_tables = ['A_BM_XZQH']
    to_tablespaces = ['GZGS_GY']
    
    # Parameter for exp
    __exp_user = 'user/password'
    __exp_dir = dbconfig.oracle['BACKUP_DIR']
    exp_type = 'byuser'
    exp_parameter = ''
    # Parameter for imp
    __imp_user = 'user/password'
    __imp_dir = dbconfig.oracle['BACKUP_DIR']
    imp_type = 'byuser'
    imp_parameter = ''
    # Parameter for expdp
    __expdp_user = 'user/password'
    __expdp_dir = 'BACKUP'
    expdp_type = 'byuser'
    expdp_parameter = 'parallel=16 cluster=n REUSE_DUMPFILES=Y'
    # Parameter for impdp
    __impdp_user = 'user/password'
    __impdp_dir = 'BACKUP'
    impdp_type = 'byuser'
    impdp_parameter = 'parallel=16 cluster=n REUSE_DUMPFILES=Y'

    def query_scn(self):
        sql = '''select dbms_flashback.get_system_change_number, 
        SCN_TO_TIMESTAMP(dbms_flashback.get_system_change_number) from dual'''
        return [(row[0], row[1]) for row in self.query_sql(sql)]
    
    def backup_restore(self, br_method='exp', br_type='byuser'):
        {
        'exp': lambda: self.exp(br_type),
        'expdp': lambda: self.expdp(br_type),
        'imp':lambda:self.imp(br_type),
        'impdp':lambda:self.impdp(br_type)
        }[br_method]()
        
    def exp(self, backup_type, backup_parameter=exp_parameter, backup_path=__exp_dir, backup_user=__exp_user):
        if backup_type.lower() == 'byuser':
            for i in self.from_users:
                text = "exp %s owner=%s file=%s log=%s %s"
                cmd = texthandler.TextHandler().format_text(
                    text, backup_user, i, backup_path + '/' + i + '_exp_imp_byuser_' + 
                    datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                    backup_path + '/' + i + '_exp_byuser_' + 
                    datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif backup_type.lower() == 'bytable':
            for i in self.from_users:
                for k in self.from_tables:
                    text = "exp %s table=%s file=%s log=%s %s"
                    cmd = texthandler.TextHandler().format_text(
                        text, backup_user, i + '.' + k, backup_path + '/' + i + '.' + k + '_exp_imp_bytable_' + 
                        datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                        backup_path + '/' + i + '.' + k + '_exp_bytable_' + 
                        datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif backup_type.lower() == 'byfull':
            text = "exp %s full=y file=%s log=%s %s"
            cmd = texthandler.TextHandler().format_text(
                text, backup_user,
                backup_path + '/' + dbconfig.oracle['instance'] + '_exp_imp_byfull_' + 
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                backup_path + '/' + dbconfig.oracle['instance'] + '_exp_byfull_' + 
                datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        else:
            logwrite.LogWrite(
                logmessage="Please asign the type of backup data. Like byuser|bytable|byfull.",
                loglevel='warnLogger').write_log()
                
    def expdp(self, backup_type, backup_parameter=expdp_parameter, backup_path=__expdp_dir, backup_user=__expdp_user):
        if backup_type.lower() == 'byuser':
            for i in self.from_users:
                text = "expdp %s schemas=%s directory=%s dumpfile=%s logfile=%s  %s"
                cmd = texthandler.TextHandler().format_text(
                    text, backup_user, i, backup_path, i + '_expdp_impdp_byuser_' + 
                    datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',
                    i + '_expdp_byuser_' + datetime.datetime.now().strftime('%Y%m%d') + 
                    '.log', backup_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif backup_type.lower() == 'bytable':
            for i in self.from_users:
                for k in self.from_tables:
                    text = "expdp %s tables=%s directory=%s dumpfile=%s logfile=%s  %s"
                    cmd = texthandler.TextHandler().format_text(
                        text, backup_user, i + '.' + k, backup_path, i + '.' + k + '_expdp_impdp_bytable_' + 
                        datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',
                        i + '.' + k + '_expdp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + 
                        '.log', backup_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif backup_type.lower() == 'bytablespace':
            for i in self.from_tablespaces:
                text = "expdp %s tablespaces=%s directory=%s dumpfile=%s logfile=%s  %s"
                cmd = texthandler.TextHandler().format_text(
                    text, backup_user, i, backup_path, i + '_expdp_impdp_bytablespace_' + 
                    datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',
                    i + '_expdp_bytablespace_' + datetime.datetime.now().strftime('%Y%m%d') + 
                    '.log', backup_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif backup_type.lower() == 'byfull':
            text = "expdp %s full=y directory=%s dumpfile=%s logfile=%s  %s"
            cmd = texthandler.TextHandler().format_text(text, backup_user, backup_path, dbconfig.oracle['instance'] + 
                '_expdp_impdp_byfull_' + datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',
                dbconfig.oracle['instance'] + '_expdp_byfull_' + 
                datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        else:
            logwrite.LogWrite(
                logmessage="Please asign the type to backup data.Like byuser|bytable|bytablespace|byfull.",
                loglevel='warnLogger').write_log() 
                
                
    def imp(self, restore_type, restore_parameter=imp_parameter, restore_path=__imp_dir, restore_user=__imp_user):
        if restore_type.lower() == 'byuser':
            for (i, j) in zip(self.from_users, self.to_users):
                text = "imp %s fromuser=%s touser=%s file=%s log=%s %s"
                cmd = texthandler.TextHandler().format_text(
                    text, restore_user, i, j, restore_path + '/' + i + '_exp_imp_byuser_' + 
                    datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                    restore_path + '/' + i + '_imp_byuser_' + 
                    datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif restore_type.lower() == 'bytable':
            for (i, j) in zip(self.from_users, self.to_users):
                for k in self.from_tables:
                    text = "imp %s fromuser=%s touser=%s table=%s file=%s log=%s %s"
                    cmd = texthandler.TextHandler().format_text(
                        text, restore_user, i, j, k, restore_path + '/' + i + '.' + k + '_exp_imp_bytable_' + 
                        datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                        restore_path + '/' + i + '.' + k + '_imp_bytable_' + 
                        datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif restore_type.lower() == 'byfull':
            text = "imp %s full=y file=%s log=%s ignore=y"
            cmd = texthandler.TextHandler().format_text(
                text, restore_user,
                restore_path + '/' + dbconfig.oracle['instance'] + '_exp_imp_byfull_' + 
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                restore_path + '/' + dbconfig.oracle['instance'] + '_imp_byfull_' + 
                datetime.datetime.now().strftime('%Y%m%d') + '.log')
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        else:
            logwrite.LogWrite(
                logmessage="Please asign the type of import data.Like byuser|bytable|byfull.",
                loglevel='warnLogger').write_log()
                
    def impdp(self, restore_type, restore_parameter=impdp_parameter, restore_path=__impdp_dir, restore_user=__impdp_user):
        if restore_type.lower() == 'byuser':
            for (i, j) in zip(self.from_users, self.to_users):
                text = "impdp %s remap_schemma=%s directory=%s dumpfile=%s logfile=%s  %s"
                cmd = texthandler.TextHandler().format_text(
                   text, restore_user, i + ':' + j, restore_path, i + '_expdp_impdp_byuser_' + 
                   datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',
                   i + '_impdp_byuser_' + datetime.datetime.now().strftime('%Y%m%d') + 
                   '.log', restore_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif restore_type.lower() == 'bytable':
            for (i , j) in zip(self.from_users, self.to_users): 
                for (k, m) in zip(self.from_tables, self.to_tables):
                    text = "impdp %s tables=%s remap_schemma=%s directory=%s dumpfile=%s logfile=%s  %s"
                    cmd = texthandler.TextHandler().format_text(
                       text, restore_user, k, i + ':' + j, restore_path, i + '.' + k + '_expdp_impdp_bytable_' + 
                       datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',
                       i + '.' + k + '_impdp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + 
                       '.log', restore_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif restore_type.lower() == 'bytablespace':
            for (i , j) in zip(self.from_users, self.to_users): 
                for (k, m) in zip(self.from_tablespaces, self.to_tablespaces):
                    text = "impdp %s remap_schemma=%s remap_tablespace=%s directory=%s dumpfile=%s logfile=%s  %s"
                    cmd = texthandler.TextHandler().format_text(
                        text, restore_user, i + ':' + j, k + ':' + m, restore_path, k + '_expdp_impdp_bytablespace_' + 
                        datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                        k + '_impdp_bytablespace_' + datetime.datetime.now().strftime('%Y%m%d') + 
                        '.log', restore_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        elif restore_type.lower() == 'byfull':
            text = "impdp %s full=y directory=%s dumpfile=%s logfile=%s  %s"
            cmd = texthandler.TextHandler().format_text(
               text, restore_user, restore_path, dbconfig.oracle['instance'] + 
               '_expdp_impdp_byfull_' + datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',
               dbconfig.oracle['instance'] + '_impdp_byfull_' + 
               datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
            logwrite.LogWrite(logmessage=cmd, loglevel='infoLogger').write_log()
        else:
            logwrite.LogWrite(
               logmessage="Please asign the type to import data.Like byuser|bytable|bytablespace|byfull.",
               loglevel='warnLogger').write_log()
                
if __name__ == '__main__':
    #DataBackupRestore().backup_restore(br_method='expdp', br_type='byuser')
    #print(Oracle().select('select username from all_users'))
    Oracle().execute("update gzgs_gy.a_bm_xzqh set nr=:1 where bm=:2", ('贵阳市','520100'),commit=True)

