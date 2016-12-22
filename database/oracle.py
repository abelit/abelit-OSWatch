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

    def execute(self, sql, bindvars='', many=False, commit=False):
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

    def __init__(self):
        None

    def select(self):
        sql = """select * from dba_users"""
        # Excute sql to query all users from view or table of the dba_users
        results = oracle.Oracle().select(sql)

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
        oracle.Oracle().execute(sql)

        # logwrite.LogWrite(loglevel='infoLogger', logmessage=sql).write_log()
        return sql

    def drop(self, username):
        sql = """DROP USER {0} cascade""".format(username)
        # Excute sql to drop user (parameter 'cascade' will drop it's objects when drop user)
        oracle.Oracle().execute(sql)

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
        oracle.Oracle().execute(sql)

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
        Oracle().execute(sql, bindvars)

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
        pass



class Lock(object):
    """docstring for Locks"""
    def select(self):
        pass

    def close(self):
        sql = """ALTER SYSTEM KILL SESSION '{0},{1}'"""

class OracleBackup(object):
    script_path = config.script['scriptpath']

    from_users = config.backup['from_users']
    from_tables = config.backup['from_tables']
    from_tablespaces = config.backup['from_tables']

    to_users = config.backup['to_users']
    to_tables = config.backup['to_tables']
    to_tablespaces = config.backup['to_tablespaces']

    def exp(self, backup_type=config.backup['exp']['backup_type'], \
            backup_parameter=config.backup['exp']['backup_parameter'], \
            backup_path=config.backup['exp']['backup_path'], \
            backup_user=config.backup['exp']['backup_user']):

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
                    text = "exp {0} tables={1} file={2} log={3} {4}".format(\
                        backup_user, i + '.' + k, backup_path + '/' + i + \
                        '.' + k + '_exp_imp_bytable_' + datetime.datetime.now().strftime('%Y%m%d') + \
                        '.dmp',backup_path + '/' + i + '.' + k + '_exp_bytable_' + \
                        datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
        elif backup_type.lower() == 'byfull':
            text = "exp {0} full=y file={1} log={2} {3}".format(backup_user,
                backup_path + '/' + config.oracle['instance'] + '_exp_imp_byfull_' +
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',
                backup_path + '/' + config.oracle['instance'] + '_exp_byfull_' +
                datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
        else:
            text = "Please asign the type of backup. Such as byuser|bytable|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()

    def expdp(self, backup_type=config.backup['expdp']['backup_type'], \
            backup_parameter=config.backup['expdp']['backup_parameter'], \
            backup_path=config.backup['expdp']['backup_path'], \
            backup_user=config.backup['expdp']['backup_user']):

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
                backup_user, backup_path, config.oracle['instance'] + '_expdp_impdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                config.oracle['instance'] + '_expdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', backup_parameter)
        else:
            text = "Please asign the type to backup data.Such as byuser|bytable|bytablespace|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()

class OracleRecovery(object):
    script_path = config.script['scriptpath']

    from_users = config.backup['from_users']
    from_tables = config.backup['from_tables']
    from_tablespaces = config.backup['from_tables']

    to_users = config.backup['to_users']
    to_tables = config.backup['to_tables']
    to_tablespaces = config.backup['to_tablespaces']

    def imp(self, restore_type=config.backup['imp']['restore_type'], \
            restore_parameter=config.backup['imp']['restore_parameter'], \
            restore_path=config.backup['imp']['restore_path'], \
            restore_user=config.backup['imp']['restore_user']):

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
                restore_user,restore_path + '/' + config.oracle['instance'] + '_exp_imp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.dmp',restore_path + '/' + \
                config.oracle['instance'] + '_imp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log')
        else:
            text = "Please asign the type of import data.Like byuser|bytable|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()

    def impdp(self, restore_type=config.backup['impdp']['restore_type'], \
            restore_parameter=config.backup['impdp']['restore_parameter'], \
            restore_path=config.backup['impdp']['restore_path'], \
            restore_user=config.backup['impdp']['restore_user']):

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
                restore_user, restore_path, config.oracle['instance'] + \
                '_expdp_impdp_byfull_' + datetime.datetime.now().strftime('%Y%m%d') + '_%U.dmp',\
                config.oracle['instance'] + '_impdp_byfull_' + \
                datetime.datetime.now().strftime('%Y%m%d') + '.log', restore_parameter)
        else:
            text = "Please asign the type to import data.Like byuser|bytable|bytablespace|byfull."
            loglevel = 'warnLogger'

        logwrite.LogWrite(logmessage=text, loglevel=loglevel).write_log()


if __name__ == '__main__':
    print(OracleBackup().expdp(backup_type='bytablespace'))
    if Table().fieldname('A_QYMC', 'GZGS_HZ') == Table().fieldname('A_QYMC', 'GZGS_GY'):
        print("yes")
    else:
        print("no")
