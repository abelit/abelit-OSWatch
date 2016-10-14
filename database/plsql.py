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

# Import customized modules
from oswatch import logwrite
from database import oracle

class Trigger(object):
    """docstring for Trigger"""
    def create_sync_trigger( self, trigger_name, tablesrc, tabledst, ownersrc, ownerdst ):
        tablesrc_pk = oracle.Table().primarykey(tablesrc, ownersrc)
        tablesrc_field = oracle.Table().field(tablesrc, ownersrc)
        # Create trigger sql
        trigger_sql = """
            create or replace trigger %s
            after insert or update or delete on %s
            for each row
            begin
                if deleting then
                    dbms_output.put_line('deleting');
                    delete from %s where %s;
                elsif inserting then
                    dbms_output.put_line('inserting');
                    insert into %s values(%s);
                elsif updating then
                    dbms_output.put_line('updating');
                    update %s set %s where %s;
                end if;
            end %s;"""

        if len(column_pk):
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
            trigger_sql=texthandler.TextHandler().format_text( trigger_text, str1, str2, str3, str4, str5, str6, str7, str8, str9, str10 )
            logwrite.LogWrite(logmessage=trigger_sql,loglevel='infoLogger')
        else:
            logwrite.LogWrite(logmessage='No primary key on the table!',loglevel='errorLogger').write_log()

class Function(object):
    pass

class Procedure(object):
    pass

class Package(object):
    pass


if __name__ == '__main__':
    DBTrigger().create_datasync_trigger( 'syncdata', 'A_BM_XZQH', 'A_BM_XZQH', 'GZGS_GY', 'GZGS_HZ' )
