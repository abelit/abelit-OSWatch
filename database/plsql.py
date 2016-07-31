# encoding: utf-8
'''
@project: __oswatch__
@modules: database.trigger
@description:
@created: Jul 31, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

"""
Import customized modules
"""
from core import logwrite
from core import texthandler
from database import db

class DBTrigger( object ):
    """docstring for Trigger"""       
    def create_datasync_trigger( self, trigger_name, tablesrc, tabledst, ownersrc, ownerdst ):
        column_pk = db.Tables().query_column(tablesrc, ownersrc)['column_pk']
        column_nm = db.Tables().query_column(tablesrc, ownersrc)['column_nm']
        # Create trigger sql
        trigger_text = '''
        create or replace trigger %s
        after insert or update or delete on %s
        for each row
        begin
        if deleting then
        dbms_output.put_line('deleting');
        delete from %s where %s;
        end if;
        if inserting then
        dbms_output.put_line('inserting');
        insert into %s
        values(%s);
        end if;
        if updating then
        dbms_output.put_line('updating');
        update %s set %s where %s;
        end if;
        end %s;
        '''
        if len( column_pk ):
            str1 = trigger_name
            str2 = ownersrc + '.' + tablesrc
            str3 = ownerdst + '.' + tabledst
            str4 = column_pk[0] + '=:old.' + column_pk[0]
            str5 = ownerdst + '.' + tabledst
            str6 = ''
            for row in range( 0, len( column_nm ) - 1 ):
                str6 = str6 + ':new.' + column_nm[row] + ','
            str6 = str6 + ':new.' + column_nm[len( column_nm ) - 1]

            str7 = ownerdst + '.' + tabledst
            str8 = ''
            for row in range( 0, len( column_nm ) - 1 ):
                str8 = str8 + column_nm[row] + '=:new.' + column_nm[row] + ','
            str8 = str8 + column_nm[len( column_nm ) - 1] + '=:new.' + column_nm[len( column_nm ) - 1]

            str9 = column_pk[0] + '=:old.' + column_pk[0]
            str10 = trigger_name
            """ Format trigger sql """
            trigger_sql=texthandler.TextHandler().format_text( trigger_text, str1, str2, str3, str4, str5, str6, str7, str8, str9, str10 )
            logwrite.LogWrite(logmessage=trigger_sql,loglevel='infoLogger') 
        else:
            logwrite.LogWrite( logmessage='No primary key on the table!',loglevel='errorLogger').write_log()

class DBFunction(object):
    pass

class DBProcedure(object):
    pass

class 


if __name__ == '__main__':
    DBTrigger().create_datasync_trigger( 'syncdata', 'A_BM_XZQH', 'A_BM_XZQH', 'GZGS_GY', 'GZGS_HZ' )

