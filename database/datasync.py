# encoding: utf-8
'''
@project: __oswatch__
@modules: database.datasync
@description:
@created: Jul 31, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

import cx_Oracle  # @UnresolvedImport

"""Import customized modules"""
from database import db
from core import texthandler
from core import logwrite
class DataSync(object):
    def sync_data(self,method,tablesrc,tabledst,ownersrc,ownerdst,condition):
        column_pk_src=db.Tables().query_column(tablesrc,ownersrc)['column_pk']
        column_nm_src=db.Tables().query_column(tablesrc,ownersrc)['column_nm']
        column_pk_dst=db.Tables().query_column(tabledst,ownerdst)['column_pk']
        column_nm_dst=db.Tables().query_column(tabledst,ownerdst)['column_nm']
        # Using merge method to synchorize data
        sql_merge='''
        MERGE INTO %s dst  USING %s src ON ( %s = %s )
        WHEN MATCHED THEN
        UPDATE SET %s %s
        WHEN NOT MATCHED THEN
        INSERT VALUES (%s)
        '''
        sql_diff='''
        select %s from (select * from %s %s minus select * from %s %s)
        '''
        sql_sync_delete='''
        delete from %s where %s in (select %s from (select * from %s %s minus select * from %s %s))
        '''
        sql_sync_insert='''
        insert into %s (select * from %s %s minus select * from %s %s)
        '''
        if column_nm_src==column_nm_dst:
            # If the table has the primary key
            if len(column_pk_src) and len(column_pk_dst) and column_pk_src==column_pk_dst:
                # Query the diffrence data between two table
                sql_diff_result=db.SQLQuery().query_sql(texthandler.TextHandler().format_text(sql_diff,column_pk_src[0],ownersrc+'.'+tablesrc,condition,ownerdst+'.'+tabledst,condition))
                if sql_diff_result:
                    sql_diff_result=sql_diff_result[0]
                    data_diff=db.SQLQuery().query_sql(texthandler.TextHandler().format_text(sql_diff,'*',ownersrc+'.'+tablesrc,condition,ownerdst+'.'+tabledst,condition))
                    sql_diff_str="("
                    for i in range(len(sql_diff_result)-1):
                        sql_diff_str=sql_diff_str+"'"+sql_diff_result[i]+"'"+","
                    sql_diff_str=sql_diff_str+sql_diff_result[len(sql_diff_result)-1]+")"
                    condition=condition+' '+'WHERE'+' '+column_pk_src[0]+' '+'IN'+' '+sql_diff_str
                    # Using merge sync data
                    if method=='merge':
                        sql_merge_col1=''
                        for row in range(1,len(column_nm_src)-1):
                            sql_merge_col1=sql_merge_col1+'dst'+'.'+column_nm_src[row]+'='+'src'+'.'+column_nm_src[row]+','
                        sql_merge_col1=sql_merge_col1+'dst'+'.'+column_nm_src[len(column_nm_src)-1]+'='+'src'+'.'+column_nm_src[len(column_nm_src)-1]
                        
                        sql_merge_col2=''
                        for row in range(len(column_nm_src)-1):
                            sql_merge_col2=sql_merge_col2+'src'+'.'+column_nm_src[row]+','
                        sql_merge_col2=sql_merge_col2+'src'+'.'+column_nm_src[len(column_nm_src)-1]
                        
                        sql_merge=texthandler.TextHandler().format_text(sql_merge,ownerdst+'.'+tabledst,ownersrc+'.'+tablesrc,'dst'+'.'+column_pk_src[0],'src'+'.'+column_pk_src[0],sql_merge_col1,condition,sql_merge_col2)
                  
                        try:
                            # Call logwrite modules to write different data into log
                            logwrite.LogWrite(logmessage=data_diff, loglevel='infoLogger').write_log()
                            # Call modules to excute sql
                            db.SQLQuery().query_sql(sql_merge,isresult=False)
                            # Call logwrite modules to write log
                            logwrite.LogWrite(logmessage=sql_merge, loglevel='infoLogger').write_log()
                        except cx_Oracle.DatabaseError:
                            logwrite.LogWrite(logmessage="Sync data error", loglevel='errorLogger').write_log()
                        else:
                            logwrite.LogWrite(logmessage="Sync data successfully between "+ownersrc+'.'+tablesrc+' and '+ownerdst+'.'+tabledst, loglevel='infoLogger').write_log()    
                    # Using minus,delete,insert sync data
                    elif method=='insert':
                        # Call logwrite modules to write different data into log
                        logwrite.LogWrite(logmessage=data_diff, loglevel='infoLogger').write_log()
                        # Delete data
                        sql_sync_delete=texthandler.TextHandler().format_text(sql_sync_delete,ownerdst+'.'+tabledst,column_pk_dst[0],column_pk_dst[0],ownersrc+'.'+tablesrc,condition,ownerdst+'.'+tabledst,condition)
                        # Insert data
                        sql_sync_insert=texthandler.TextHandler().format_text(sql_sync_insert,ownerdst+'.'+tabledst,ownersrc+'.'+tablesrc,condition,ownerdst+'.'+tabledst,condition)
                        try:
                            db.SQLQuery().query_sql(sql_sync_delete,isresult=False)
                            db.SQLQuery().query_sql(sql_sync_insert,isresult=False)
                            logwrite.LogWrite(logmessage='The sql to sync data: '+sql_sync_delete+sql_sync_insert, loglevel='infoLogger').write_log()
                        except cx_Oracle.DatabaseError:
                            logwrite.LogWrite(logmessage="Sync data error", loglevel='errorLogger').write_log()
                        else:
                            logwrite.LogWrite(logmessage="Sync data successfully between "+ownersrc+'.'+tablesrc+' and '+ownerdst+'.'+tabledst, loglevel='infoLogger').write_log()
                    else:
                        logwrite.LogWrite(logmessage="Please input 'insert' or 'merge' to sync data,like syncdata(merge)", loglevel='errorLogger').write_log()
                else:
                    logwrite.LogWrite(logmessage="No data need to syncthonize between "+ownersrc+'.'+tablesrc+' and '+ownerdst+'.'+tabledst, loglevel='warnLogger').write_log()
            else:
                logwrite.LogWrite(logmessage="Empty list, no primary key on table "+ownersrc+'.'+tablesrc+' or '+ownersrc+'.'+tablesrc, loglevel='errorLogger'+' or there are diffrences on two tables').write_log()
  
class DataCompare(object):
    """class DataCompare Doc"""
    def compare_data(self):
        pass
    
# Call function to sync data
if __name__=='__main__':
    tablesrc='A_BM_XZQH'
    tabledst='A_BM_XZQH'
    ownersrc='GZGS_GY'
    ownerdst='GZGS_HZ'
    condition=""
    method='merge'
    DataSync().sync_data(method=method,tablesrc=tablesrc,tabledst=tabledst,ownersrc=ownersrc,ownerdst=ownerdst,condition=condition)