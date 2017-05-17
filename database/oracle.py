# encoding: utf-8
"""
@project: __oswatch__
@modules: database.oracle
@description:

@created:Sep 22, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

"""

import datetime
import os
# Import module cx_Oracle to connect oracle using python
try:
    import cx_Oracle
except ImportError:
    cx_oracle_exists = False
else:
    cx_oracle_exists = True

# Import customized modules
from oswatch import logwrite

# Module dbocnfig in the package of config to configure
from config import config


class Oracle:
    def __init__(self, *args):
        """
        Function: __init__
        Summary: InsertHere
        Examples: InsertHere
        Attributes:
            @param (self):InsertHere
            @param (*args):InsertHere
            username: oracle user,
            password: oracle user's password,
            mod:  "normal, sysdba, sysoper",defaut is normal,
            host: the oracle database locates
            port: the port listen oracle database service, config is 1521
            insrance: the service name of the oracle database instance
        Returns: InsertHere
        """
        self.username = config.oracle['username']
        self.password = config.oracle['password']
        self.mode     = config.oracle['mode']
        self.host     = config.oracle['host']
        self.port     = config.oracle['port']
        self.instance = config.oracle['instance']

    def __connect(self):
        """
        Function: __connect
        Summary: InsertHere
        Examples: self.__connect()
        Attributes:
            @param (self):Call __connect metod Oracle.__connect()
        Returns: connection
        """
        # Set lang environment
        NLS_LANG = config.oracle['NLS_LANG']
        os.environ['NLS_LANG'] = NLS_LANG

        if not cx_oracle_exists:
            msg = """The cx_Oracle module is required. 'pip install cx_Oracle' \
                should do the trick. If cx_Oracle is installed, make sure ORACLE_HOME \
                & LD_LIBRARY_PATH is set"""
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

    def execute(self, sql, bindvars='', many=False, commit=True):
        """
        Function: execute
        Summary: Execute whatever SQL statements are passed to the method;
            commit if specified. Do not specify fetchall() in here as
            the SQL statement may not be a select.
            bindvars is a dictionary of variables you pass to execute.
        Examples: Oracle().execute(...)
        Attributes:
            @param (self):class method
            @param (sql):The sql that will be excuted
            @param (bindvars) config='': The bind variables of sql
            @param (many) config=False: If set many is True, multiple sql will be excuted at the same time
            @param (commit) config=False: False is not needed commit after excuting sql, but True is needed commit, gernerally DML sql
        Returns: NO value will be returned
        """
        # if ('insert' or 'delete' or 'update' in sql.lower()) or commit:
        #     commit = True
        # else:
        #     commit = False
        try:
            self.__connect()
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
            self.__disconnect()

class User(object):

    def __init__(self):
        None

    def select(self):
        sql = """select * from dba_users"""
        # Excute sql to query all users from view or table of the dba_users
        results = Oracle().select(sql)

        return results

    def create(self, username, password, default_tablespace=None, temporary_tablespace=None):
        sql = """CREATE USER {0} IDENTIFIED BY {1}"""

        # Create config tablespace if needs when create user, and config tablespace is user if no tablespace to be assigned
        if default_tablespace is not None:
            sql = sql+" DEFAULT TABLESPACE "+default_tablespace

        # Create temporary tablespace if needs when create user, and config temporary tablespace is temp if no tablespace to be assigned
        if temporary_tablespace is not None:
            sql = sql+" TEMPORARY TABLESPACE "+temporary_tablespace

        sql = sql.format(username, password)
        # Excute sql to create user
        Oracle().execute(sql)

        # logwrite.LogWrite(loglevel='infoLogger', logmessage=sql).write_log()
        return sql

    def drop(self, username):
        sql = """DROP USER {0} cascade""".format(username)
        # Excute sql to drop user (parameter 'cascade' will drop it's objects when drop user)
        Oracle().execute(sql)

        return sql

    def alter(self, username, password=None, default_tablespace=None, temporary_tablespace=None):
        sql = """ALTER USER {0}"""
        if password is not None:
            sql = sql+" IDENTIFIED BY "+password

        if default_tablespace is not None:
            sql = sql+" DEFAULT TABLESPACE "+default_tablespace

        if temporary_tablespace is not None:
            sql = sql+"TEMPORARY TABLESPACE "+temporary_tablespace

        sql = sql.format(username)
        # Excute sql to alter user's properties
        Oracle().execute(sql)

        return sql

class Table(object):
    def field(self, tablename, owner):
        # Query all columns of the table
        sql = """select column_name from all_tab_columns \
            where table_name = :1 and owner = :2"""
        results = Oracle().select(sql, (tablename, owner))

        return results

    def primarykey(self, tablename, owner):
        # Query primary key of the table
        sql = """select col.column_name  from all_constraints con, \
            all_cons_columns col where con.constraint_name = col.constraint_name \
            and con.constraint_type='P' and col.table_name = :1 \
            and col.owner = :2 and con.owner=col.owner"""
        results = Oracle().select(sql, (tablename, owner))
        return results

    def tables(self):
        sql =  """SELECT * FROM ALL_TABLES"""
        results = Oracle().select(sql)

        return results

    def select(self, tablename, fields=None):
        sql = """select {0} from {1}"""

        if fields is not None:
            fields = ','.join(fields)
            sql = sql.format(fields, tablename)
        else:
            sql = sql.format('*', tablename)

        results = Oracle().select(sql)
        return results

    def update(self, sql, bindvars=''):
        """Usage: UPDATE {tablename} SET {field_name}={new value} WHERE {key field}={value}"""
        Oracle().execute(sql, bindvars, commit=True)

    def delete(self, table_name, primary_key):
        """Usage: DELETE FROM {tablename} WHERE {key field}={value}"""
        sql = """DELETE FROM {0} {1}"""
        Oracle().execute(sql)

    def insert(self, sql, bindvars=''):
        """Usage: INSERT INTO {tablename} VALUES {(fields...)}"""
        Oracle().execute(sql, bindvars, commit=True)

    def create(self, sql):
        """Usage: CREATE TABLE {tablename}"""
        Oracle().execute(sql)

    def drop(self, tablename):
        """Usage: DROP TABLE {tablename} PURGE"""
        sql = "DROP TABLE "+tablename+" purge"
        Oracle().execute(sql)

    def alter(self, tablename):
        """
        Function: alter
        Summary: 1.Rename table: ALTER TABLE {old_tablename} RENAME TO {new_tablename};
                 2.Add one column: ALTER TABLE {tablename} ADD {field_name} {field_type};
                 3.Add many columns: ALTER TABLE {tablename} ADD ({field1_name} {field1_type},{field2_name} {field2_type},...);
                 4.Modify column: ALTER TABLE {tablename} MODIFY {field_name} {new_field_type};
                 5.Modify many columns: ALTER TABLE {tablename} MODIFY ({field1_name} {new_field1_type},{field2_name} {new_field2_type},...);
                 6.Remove column: ALTER TABLE {tablename} DROP COLUMN {field_name}
                 7.Rename column: ALTER TABLE {tablename} RENAME COLUMN {field_name} TO {new_field_name}
        Examples: ...
        Attributes:
            @param (self):InsertHere
            @param (tablename):InsertHere
        Returns: InsertHere
        """
        pass


class Tablespace(object):

    def select(self, tablespace_name=None):
        """
        Function: select
        Summary: Get tablespace usage
            Monitor  details of the use of tablespace
        Examples: Tablespace().select()
        Attributes:
            @param (self):InsertHere
        Returns: size and percent of total_mb,used_mb,free_mb of all tablespaces
        """
        sql = """SELECT a.tablespace_name, \
            a.bytes/(1024*1024) total_mb, \
            b.bytes/(1024*1024) used_mb, \
            c.bytes/1024/1024 free_mb, \
            round((b.bytes * 100) / a.bytes,2) used_pct, \
            round((c.bytes * 100) / a.bytes,2) free_pct \
            FROM sys.sm$ts_avail a, sys.sm$ts_used b, sys.sm$ts_free c \
            WHERE a.tablespace_name = b.tablespace_name \
            AND a.tablespace_name = c.tablespace_name"""
        if tablespace_name is not None:
            sql = sql+" a.tablespace_name="+tablespace_name
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
        sql = """SELECT file_id,file_name,online_status,tablespace_name, \
            ROUND(bytes / (1024 * 1024), 2) total_space_mb,autoextensible \
            FROM dba_data_files"""
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
        sql = """select * from v$logfile"""
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
    def select(self):
        """DocStrins: gv$session and v$session both are views in the oracle,
        but gv$session shows global rac sessions,including all instance """
        sql = """select count(*) from gv$session where username is not null \
            and status='ACTIVE"""
        params = {'status':'ACTIVE'}
        # return [(row[1],row[2]) for row in self.query_sql(sql,params)]
        return {
            'session_counts':len(self.query_sql(sql, params)),
            'session_sid_serial':[(row[1], row[2]) for row in self.query_sql(sql, params)]
        }

    def close(self):
        #sql = """ALTER SYSTEM KILL SESSION '{0},{1}'"""
        pass

class Lock(object):
    """docstring for Locks"""
    def select(self):
        pass

    def close(self):
        pass

class OracleBackup(object):
    def exp(self, backup_user=config.backup['exp']['backup_user'], \
            exp_type=config.backup['exp']['exp_type'], \
            exp_parameter=config.backup['exp']['exp_parameter'], \
            exp_path=config.backup['exp']['exp_path'], \
            exp_table=config.backup['exp']['exp_table'], \
            exp_user=config.backup['exp']['exp_user']):

        loglevel = 'infoLogger'
        text     = 'No exp sql assigned here!'
        
        if exp_type.lower() == 'byuser':
            text = 'exp {0} owner={1} file={2} log={3} {4}'.format(\
                backup_user, exp_user, exp_path+'/'+exp_user+'_exp_imp_byuser_'+ \
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',exp_path + '/' + \
                exp_user+'_exp_byuser_'+datetime.datetime.now().strftime('%Y%m%d')+'.log', \
                exp_parameter)
        elif exp_type.lower() == 'bytable':
            text = "exp {0} tables={1} file={2} log={3} {4}".format(\
                backup_user, exp_user + '.' + exp_table, exp_path + '/' + exp_user + \
                '.' + exp_table + '_exp_imp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + \
                '.dmp',exp_path + '/' + exp_user + '.' + exp_table + '_exp_bytable_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', exp_parameter)
        elif exp_type.lower() == 'byfull':
            text = "exp {0} full=y file={1} log={2} {3}".format(\
                backup_user,exp_path + '/' + config.oracle['instance'] + '_exp_imp_byfull_' +
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                exp_path + '/' + config.oracle['instance'] + '_exp_byfull_' +
                datetime.datetime.now().strftime('%Y%m%d') + '.log', exp_parameter)
        else:
            text = "Please asign the type of exp. Such as byuser|bytable|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()

    def expdp(self, backup_user=config.backup['expdp']['backup_user'], \
            expdp_type=config.backup['expdp']['expdp_type'], \
            expdp_parameter=config.backup['expdp']['expdp_parameter'], \
            expdp_path=config.backup['expdp']['expdp_path'], \
            expdp_table=config.backup['expdp']['expdp_table'], \
            expdp_schema=config.backup['expdp']['expdp_schema'], \
            expdp_tablespace=config.backup['expdp']['expdp_tablespace']):

        loglevel = 'infoLogger'
        text     = 'No expdp sql assigned here!'
        
        if expdp_type.lower() == 'byschema':
            text = "expdp {0} schemas={1} directory={2} dumpfile={3} logfile={4} {5}".format(\
                backup_user, expdp_schema, expdp_path, expdp_schema + '_expdp_impdp_byschema_' + \
                datetime.datetime.now().strftime('%Y%m%d%H') + '_%U.dmp',\
                expdp_schema + '_expdp_byschema_' + datetime.datetime.now().strftime('%Y%m%d%H') + '.log', \
                expdp_parameter)
        elif expdp_type.lower() == 'bytable':
            text = "expdp {0} tables={1} directory={2} dumpfile={3} logfile={4} {5}".format(\
                backup_user, expdp_schema + '.' + expdp_table, expdp_path, expdp_schema + '.' + expdp_table + '_expdp_impdp_bytable_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                expdp_schema + '.' + expdp_table + '_expdp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + '.log', \
                expdp_parameter)
        elif expdp_type.lower() == 'bytablespace':
            text = "expdp {0} tablespaces={1} directory={2} dumpfile={3} logfile={4} {5}".format(\
                backup_user, expdp_tablespace, expdp_path, expdp_tablespace + '_expdp_impdp_bytablespace_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                expdp_tablespace + '_expdp_bytablespace_' + datetime.datetime.now().strftime('%Y%m%d') + '.log', \
                expdp_parameter)
        elif expdp_type.lower() == 'byfull':
            text = "expdp {0} full=y directory={1} dumpfile={2} logfile={3} {4}".format(\
                backup_user, expdp_path, config.oracle['instance'] + '_expdp_impdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                config.oracle['instance'] + '_expdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', expdp_parameter)
        else:
            text = "Please asign the type to expdp data.Such as byschema|bytable|bytablespace|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()

class OracleRecovery(object):
    script_path = config.script['scriptpath']

    def imp(self, backup_user=config.backup['imp']['backup_user'], \
            imp_type=config.backup['imp']['imp_type'], \
            imp_parameter=config.backup['imp']['imp_parameter'], \
            imp_path=config.backup['imp']['imp_path'], \
            imp_table=config.backup['imp']['imp_table'], \
            from_user=config.backup['imp']['from_user'], \
            to_user=config.backup['imp']['to_user']):

        loglevel = 'infoLogger'
        text     = 'No imp sql assigned here!'
        
        if imp_type.lower() == 'byuser':
            text = "imp {0} fromuser={1} touser={2} file={3} log={4} {5}".format(\
                backup_user, from_user, to_user, imp_path + '/' + from_user + '_exp_imp_byuser_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',\
                imp_path + '/' + from_user + '_imp_byuser_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', imp_parameter)
        elif imp_type.lower() == 'bytable':
            text = "imp {0} fromuser={1} touser={2} tables={3} file={4} log={5} {6}".format(\
                backup_user, from_user, to_user, imp_table, imp_path + '/' + from_user + '.' + to_user + '_exp_imp_bytable_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',\
                imp_path + '/' + from_user + '.' + imp_table + '_imp_bytable_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', imp_parameter)
        elif imp_type.lower() == 'byfull':
            text = "imp {0} full=y file={1} log={2} ignore=y".format(\
                backup_user,imp_path + '/' + config.oracle['instance'] + '_exp_imp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',imp_path + '/' + \
                config.oracle['instance'] + '_imp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log')
        else:
            text = "Please asign the type of import data.Like byuser|bytable|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()

    def impdp(self, backup_user=config.backup['expdp']['backup_user'], \
            impdp_type=config.backup['impdp']['impdp_type'], \
            impdp_parameter=config.backup['impdp']['impdp_parameter'], \
            impdp_path=config.backup['impdp']['impdp_path'], \
            impdp_table=config.backup['impdp']['impdp_table'], \
            from_schema=config.backup['impdp']['from_schema'], \
            to_schema=config.backup['impdp']['to_schema'], \
            from_tablespace=config.backup['impdp']['from_tablespace'], \
            to_tablespace=config.backup['impdp']['to_tablespace']):

        loglevel = 'infoLogger'
        text     = 'No impdp sql assigned here!'
        
        if impdp_type.lower() == 'byschema':
            text = "impdp {0} remap_schemma={1} directory={2} dumpfile={3} logfile={4}  {5}".format(\
                backup_user, from_schema + ':' + to_schema, impdp_path, from_schema + '_expdp_impdp_byschema_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',from_schema + '_impdp_byschema_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', impdp_parameter)
        elif impdp_type.lower() == 'bytable':
            text = "impdp {0} tables={1} remap_schemma={2} directory={3} dumpfile={4} logfile={5}  {6}".format(\
                backup_user, impdp_table, from_schema + ':' + to_schema, impdp_path, from_schema + '.' + impdp_table + '_expdp_impdp_bytable_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',from_schema + '.' + impdp_table + \
                '_impdp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + '.log', impdp_parameter)
        elif impdp_type.lower() == 'bytablespace':
            text = "impdp {0} remap_schemma={1} remap_tablespace={2} directory={3} dumpfile={4} logfile={5}  {6}".format(\
                backup_user, from_schema + ':' + to_schema, from_tablespace + ':' + to_tablespace, impdp_path, from_tablespace + '_expdp_impdp_bytablespace_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',from_tablespace + '_impdp_bytablespace_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', impdp_parameter)
        elif impdp_type.lower() == 'byfull':
            text = "impdp {0} full=y directory={1} dumpfile={2} logfile={3}  {4}".format(\
                backup_user, impdp_path, config.oracle['instance'] + \
                '_expdp_impdp_byfull_' + datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                config.oracle['instance'] + '_impdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', impdp_parameter)
        else:
            text = "Please asign the type to import data.Like byschema|bytable|bytablespace|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()


if __name__ == '__main__':
    print(OracleBackup().exp(exp_type='bytable'))
