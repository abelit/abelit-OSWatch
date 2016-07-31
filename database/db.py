# encoding: utf-8
'''
@project: __oswatch__
@modules: database.db
@description:
@created: Jul 31, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

import datetime
"""
Import customized modules
"""
from database import dbconnect
from core import texthandler
from core import logwrite
from config import dbconfig

class DB( object ):      
    # Define function format_sql to replace the string for new sqltext
    pass
 
"""docstring for Oracle"""
class SQLQuery( object ):
    """docstring for SQLQuery"""
    def query_sql( self, sql, params = '', isresult = True ):
        # Connect to oracle
        conn = dbconnect.DBConnect().conn_oracle()
        # Declare cursor
        curs = conn.cursor()
        curs.execute( sql, params )
        if isresult:
            result = curs.fetchall()
            return result
        curs.execute( 'commit' )
        # Release resource
        curs.close()
        conn.close()
         
class Users( SQLQuery ):
    """docstring for Users"""
    def query_user( self ):
        sql = '''
        select * from all_users  where username like :username
        '''
        return self.query_sql( sql, {'username':'GZGS%'} )
    def create_user( self ):
        pass
     
    def drop_user( self ):
        pass
     
    def set_user( self ):
        pass
     
         
class Tables( SQLQuery ):
    """docstring for Tables"""
    # def __init__(self, arg):
    #     super(Tables, self).__init__()
    #     self.arg = arg
    def query_column( self, tablename, owner ):
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
            'column_pk':[row[0] for row in self.query_sql( sql_pk, {'table_name':tablename, 'owner':owner} )],
            'column_nm':[row[0] for row in self.query_sql( sql_nm, {'table_name':tablename, 'owner':owner} )]
        }
     
    def query_table( self ):
        pass
         
    def create_table( self ):
        pass
     
    def drop_table( self ):
        pass
     
    def set_table( self ):
        pass
     
     
class Tablespaces( object ):
    """docstring for Tablespaces"""
    def __init__( self, arg ):
        super( Tablespaces, self ).__init__()
        self.arg = arg
 
    def query_tablespace( self ):
        pass
         
    def create_tablespace( self ):
        pass
 
    def set_tablespace( self ):
        pass
 
    def drop_tablespace( self ):
        pass
 
class Datafiles( object ):
    """docstring for Datafiles"""
    def __init__( self, arg ):
        super( Datafiles, self ).__init__()
        self.arg = arg
 
    def query_datafile( self ):
        pass
 
    def create_datafile( self ):
        pass
 
    def set_datafile( self ):
        pass
         
    def drop_datafile( self ):
        pass
 
class Onlinelogs( object ):
    """docstring for Onlinelogs"""
    def __init__( self, arg ):
        super( Onlinelogs, self ).__init__()
        self.arg = arg
 
    def query_onlinelog( self ):
        pass
 
    def add_onlinelog( self ):
        pass
 
    def delete_onlinelog( self ):
        pass
 
    def set_onlinelog( self ):
        pass
         
class Archivelogs( object ):
    """docstring for Archivelogs"""
    def __init__( self, arg ):
        super( Archivelogs, self ).__init__()
        self.arg = arg
         
 
class Sessions( SQLQuery ):
    """docstring for Sessions"""
    def query_session( self ):
        sql = '''
            select * from v$session where status=:status
            '''
        params = {'status':'ACTIVE'}
        # return [(row[1],row[2]) for row in self.query_sql(sql,params)]
        return {
            'session_counts':len( self.query_sql( sql, params ) ),
            'session_sid_serial':[( row[1], row[2] ) for row in self.query_sql( sql, params )]
        }
 
    def close_session( self ):
        pass
 
         
 
class Locks( object ):
    """docstring for Locks"""
    def __init__( self, arg ):
        super( Locks, self ).__init__()
        self.arg = arg
     
    def query_lock( self ):
        pass
 
    def release_lock( self ):
        pass
     

class DataBackupRestore( object ):
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

    def query_scn( self ):
        sql = '''select dbms_flashback.get_system_change_number, 
        SCN_TO_TIMESTAMP(dbms_flashback.get_system_change_number) from dual'''
        return [( row[0], row[1] ) for row in self.query_sql( sql )]
    
    def backup_restore( self, br_method = 'exp', br_type = 'byuser' ):
        {
        'exp': lambda: self.exp( br_type ),
        'expdp': lambda: self.expdp( br_type ),
        'imp':lambda:self.imp( br_type ),
        'impdp':lambda:self.impdp( br_type )
        }[br_method]()
        
    def exp( self, backup_type, backup_parameter = exp_parameter, backup_path = __exp_dir, backup_user = __exp_user ):
        if backup_type.lower() == 'byuser':
            for i in self.from_users:
                text = "exp %s owner=%s file=%s log=%s %s"
                cmd = texthandler.TextHandler().format_text( 
                    text, backup_user, i, backup_path + '/' + i + '_exp_imp_byuser_' + 
                    datetime.datetime.now().strftime( '%Y%m%d' ) + '.dmp',
                    backup_path + '/' + i + '_exp_byuser_' + 
                    datetime.datetime.now().strftime( '%Y%m%d' ) + '.log', backup_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif backup_type.lower() == 'bytable':
            for i in self.from_users:
                for k in self.from_tables:
                    text = "exp %s table=%s file=%s log=%s %s"
                    cmd = texthandler.TextHandler().format_text( 
                        text, backup_user, i + '.' + k, backup_path + '/' + i + '.' + k + '_exp_imp_bytable_' + 
                        datetime.datetime.now().strftime( '%Y%m%d' ) + '.dmp',
                        backup_path + '/' + i + '.' + k + '_exp_bytable_' + 
                        datetime.datetime.now().strftime( '%Y%m%d' ) + '.log', backup_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif backup_type.lower() == 'byfull':
            text = "exp %s full=y file=%s log=%s %s"
            cmd = texthandler.TextHandler().format_text( 
                text, backup_user,
                backup_path + '/' + dbconfig.oracle['instance'] + '_exp_imp_byfull_' + 
                datetime.datetime.now().strftime( '%Y%m%d' ) + '.dmp',
                backup_path + '/' + dbconfig.oracle['instance'] + '_exp_byfull_' + 
                datetime.datetime.now().strftime( '%Y%m%d' ) + '.log', backup_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        else:
            logwrite.LogWrite( 
                logmessage = "Please asign the type of backup data. Like byuser|bytable|byfull.",
                loglevel = 'warnLogger' ).write_log()
                
    def expdp( self, backup_type, backup_parameter = expdp_parameter, backup_path = __expdp_dir, backup_user = __expdp_user ):
        if backup_type.lower() == 'byuser':
            for i in self.from_users:
                text = "expdp %s schemas=%s directory=%s dumpfile=%s logfile=%s  %s"
                cmd = texthandler.TextHandler().format_text( 
                    text, backup_user, i, backup_path, i + '_expdp_impdp_byuser_' + 
                    datetime.datetime.now().strftime( '%Y%m%d' ) + '_%U.dmp',
                    i + '_expdp_byuser_' + datetime.datetime.now().strftime( '%Y%m%d' ) + 
                    '.log', backup_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif backup_type.lower() == 'bytable':
            for i in self.from_users:
                for k in self.from_tables:
                    text = "expdp %s tables=%s directory=%s dumpfile=%s logfile=%s  %s"
                    cmd = texthandler.TextHandler().format_text( 
                        text, backup_user, i + '.' + k, backup_path, i + '.' + k + '_expdp_impdp_bytable_' + 
                        datetime.datetime.now().strftime( '%Y%m%d' ) + '_%U.dmp',
                        i + '.' + k + '_expdp_bytable_' + datetime.datetime.now().strftime( '%Y%m%d' ) + 
                        '.log', backup_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif backup_type.lower() == 'bytablespace':
            for i in self.from_tablespaces:
                text = "expdp %s tablespaces=%s directory=%s dumpfile=%s logfile=%s  %s"
                cmd = texthandler.TextHandler().format_text( 
                    text, backup_user, i, backup_path, i + '_expdp_impdp_bytablespace_' + 
                    datetime.datetime.now().strftime( '%Y%m%d' ) + '_%U.dmp',
                    i + '_expdp_bytablespace_' + datetime.datetime.now().strftime( '%Y%m%d' ) + 
                    '.log', backup_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif backup_type.lower() == 'byfull':
            text = "expdp %s full=y directory=%s dumpfile=%s logfile=%s  %s"
            cmd = texthandler.TextHandler().format_text( text, backup_user, backup_path, dbconfig.oracle['instance'] + 
                '_expdp_impdp_byfull_' + datetime.datetime.now().strftime( '%Y%m%d' ) + '_%U.dmp',
                dbconfig.oracle['instance'] + '_expdp_byfull_' + 
                datetime.datetime.now().strftime( '%Y%m%d' ) + '.log', backup_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        else:
            logwrite.LogWrite( 
                logmessage = "Please asign the type to backup data.Like byuser|bytable|bytablespace|byfull.",
                loglevel = 'warnLogger' ).write_log() 
                
                
    def imp( self, restore_type, restore_parameter = imp_parameter, restore_path = __imp_dir, restore_user = __imp_user ):
        if restore_type.lower() == 'byuser':
            for ( i, j ) in zip( self.from_users, self.to_users ):
                text = "imp %s fromuser=%s touser=%s file=%s log=%s %s"
                cmd = texthandler.TextHandler().format_text( 
                    text, restore_user, i, j, restore_path + '/' + i + '_exp_imp_byuser_' + 
                    datetime.datetime.now().strftime( '%Y%m%d' ) + '.dmp',
                    restore_path + '/' + i + '_imp_byuser_' + 
                    datetime.datetime.now().strftime( '%Y%m%d' ) + '.log', restore_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif restore_type.lower() == 'bytable':
            for ( i, j ) in zip( self.from_users, self.to_users ):
                for k in self.from_tables:
                    text = "imp %s fromuser=%s touser=%s table=%s file=%s log=%s %s"
                    cmd = texthandler.TextHandler().format_text( 
                        text, restore_user, i, j, k, restore_path + '/' + i + '.' + k + '_exp_imp_bytable_' + 
                        datetime.datetime.now().strftime( '%Y%m%d' ) + '.dmp',
                        restore_path + '/' + i + '.' + k + '_imp_bytable_' + 
                        datetime.datetime.now().strftime( '%Y%m%d' ) + '.log', restore_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif restore_type.lower() == 'byfull':
            text = "imp %s full=y file=%s log=%s ignore=y"
            cmd = texthandler.TextHandler().format_text( 
                text, restore_user,
                restore_path + '/' + dbconfig.oracle['instance'] + '_exp_imp_byfull_' + 
                datetime.datetime.now().strftime( '%Y%m%d' ) + '.dmp',
                restore_path + '/' + dbconfig.oracle['instance'] + '_imp_byfull_' + 
                datetime.datetime.now().strftime( '%Y%m%d' ) + '.log' )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        else:
            logwrite.LogWrite( 
                logmessage = "Please asign the type of import data.Like byuser|bytable|byfull.",
                loglevel = 'warnLogger' ).write_log()
                
    def impdp( self, restore_type, restore_parameter = impdp_parameter, restore_path = __impdp_dir, restore_user = __impdp_user ):
        if restore_type.lower() == 'byuser':
            for ( i, j ) in zip( self.from_users, self.to_users ):
                text = "impdp %s remap_schemma=%s directory=%s dumpfile=%s logfile=%s  %s"
                cmd = texthandler.TextHandler().format_text( 
                   text, restore_user, i + ':' + j, restore_path, i + '_expdp_impdp_byuser_' + 
                   datetime.datetime.now().strftime( '%Y%m%d' ) + '_%U.dmp',
                   i + '_impdp_byuser_' + datetime.datetime.now().strftime( '%Y%m%d' ) + 
                   '.log', restore_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif restore_type.lower() == 'bytable':
            for ( i , j ) in zip( self.from_users, self.to_users ): 
                for ( k, m ) in zip( self.from_tables, self.to_tables ):
                    text = "impdp %s tables=%s remap_schemma=%s directory=%s dumpfile=%s logfile=%s  %s"
                    cmd = texthandler.TextHandler().format_text( 
                       text, restore_user, k, i + ':' + j, restore_path, i + '.' + k + '_expdp_impdp_bytable_' + 
                       datetime.datetime.now().strftime( '%Y%m%d' ) + '_%U.dmp',
                       i + '.' + k + '_impdp_bytable_' + datetime.datetime.now().strftime( '%Y%m%d' ) + 
                       '.log', restore_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif restore_type.lower() == 'bytablespace':
            for ( i , j ) in zip( self.from_users, self.to_users ): 
                for ( k, m ) in zip( self.from_tablespaces, self.to_tablespaces ):
                    text = "impdp %s remap_schemma=%s remap_tablespace=%s directory=%s dumpfile=%s logfile=%s  %s"
                    cmd = texthandler.TextHandler().format_text( 
                        text, restore_user, i + ':' + j, k + ':' + m, restore_path, k + '_expdp_impdp_bytablespace_' + 
                        datetime.datetime.now().strftime( '%Y%m%d' ) + '.dmp',
                        k + '_impdp_bytablespace_' + datetime.datetime.now().strftime( '%Y%m%d' ) + 
                        '.log', restore_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        elif restore_type.lower() == 'byfull':
            text = "impdp %s full=y directory=%s dumpfile=%s logfile=%s  %s"
            cmd = texthandler.TextHandler().format_text( 
               text, restore_user, restore_path, dbconfig.oracle['instance'] + 
               '_expdp_impdp_byfull_' + datetime.datetime.now().strftime( '%Y%m%d' ) + '_%U.dmp',
               dbconfig.oracle['instance'] + '_impdp_byfull_' + 
               datetime.datetime.now().strftime( '%Y%m%d' ) + '.log', restore_parameter )
            logwrite.LogWrite( logmessage = cmd, loglevel = 'infoLogger' ).write_log()
        else:
            logwrite.LogWrite( 
               logmessage = "Please asign the type to import data.Like byuser|bytable|bytablespace|byfull.",
               loglevel = 'warnLogger' ).write_log()
                
if __name__ == '__main__':
    DataBackupRestore().backup_restore( br_method = 'expdp', br_type = 'bytablespace' )


