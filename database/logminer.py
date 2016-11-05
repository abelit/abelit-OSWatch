#!/usr/bin/python
#coding:utf-8

# Function:Synchronize data between two tables
# Description:
# Author:Abelit
# Email:ychenid@live.com
# Date:2016-07-07
import cx_Oracle
import re
import datetime
import time

from oswatch import logwrite
from sqlquery import SQLQuery

class LogMiner:
    """docstring for LogMiner"""
    def __init__(self, arg):
        super(LogMiner, self).__init__()
        self.arg = arg
        
    # logminer configuration
    logconf={
        'start_time':'2016-06-04 00:00:00',
        'end_time':'2016-06-04 23:59:59',
        'schema':'GZGS_GY',
        'tablename':'A_BM_XZQH',
        'ORACLE_SID':'gzgszxk1'
    }

    def writer(logminer_shell,lines):
        output_file=open(logminer_shell,'a',encoding='utf-8')
        output_file.write(lines)
        output_file.close()

    # Save results of analyzing logminer
    logminerdir='/u01/app/oracle/logminer'
    logminer_record='/u01/app/oracle/logminer/record.txt'
    dictfile='dictionary.ora'
    logminerdict='/u01/app/oracle/logminer/dictionary.ora'
    log='''
    select v1.member from v$logfile v1,v$log v2 where v1.group#=v2.group# and v2.archived!='YES'
    '''
    archive='''
    select name from v$archived_log where to_char(completion_time,'YYYY-MM-DD HH:mm:SS') between '%s' and '%s'
    '''
    archive_sql=SQLQuery().format_text(archive,logconf['start_time'],logconf['end_time'])
    onlinelog=SQLQuery().query_sql(log)
    archivelog=SQLQuery().query_sql(archive_sql)

    #logminer_shell='c:\\users\\abelit\\desktop\\logminer.sh'
    logminer_shell='/home/abelit/Documents/logminer.sh'
    logminer_shell_head='''
    #!/bin/ksh

    export ORACLE_SID=%s
    sqlplus / as sysdba <<!
    create or replace trigger on_logon_trigger
    after logon on database
    begin
    dbms_application_info.set_client_info(sys_context('userenv', 'ip_address'));
    end;
    /
    set echo off;
    set heading off;
    set line 100;
    set long 2000000000;
    set longchunksize 255;
    set wra on;
    set newpage none;
    set pagesize 0;
    set numwidth 12;
    set termout off;
    set trimout on;
    set trimspool on;
    set feedback off;
    set timing on;
    '''
    logminer_shell_end='''
    execute dbms_logmnr.start_logmnr(DictFileName=>'%s',starttime =>to_date('%s','YYYY-MM-DD HH24:MI:SS'),
    endtime =>to_date('%s','YYYY-MM-DD HH24:MI:SS'));
    execute dbms_logmnr.end_logmnr;
    spool '%s';
    select to_clob(sql_redo)||'|'||to_clob(sql_undo)||'|'||to_char(scn)||'|'||to_char(timestamp)||'|'||to_char(session_info)||'|'||to_char(table_name)||'|'||to_char(seg_owner)||'?'
    from v\$logmnr_contents WHERE seg_name='%s' AND seg_owner='%s';
    spool off;
    exit
    !
    '''

    #execute dbms_logmnr_d.build(dictionary_filename =>dictfile, dictionary_location =>logminerdir);

    new_log='''
    execute dbms_logmnr.add_logfile(LogFileName=>'%s',Options=>dbms_logmnr.new);
    '''
    add_log='''
    execute dbms_logmnr.add_logfile(LogFileName=>'%s',Options=>dbms_logmnr.addfile);
    '''
    writer(logminer_shell,sqlquery.format_text(logminer_shell_head,logconf['ORACLE_SID']))
    writer(logminer_shell,sqlquery.format_text(new_log,onlinelog[0]))
    for i in range(1,len(onlinelog)):
        writer(logminer_shell,sqlquery.format_text(add_log,onlinelog[i]))
    for i in range(1,len(archivelog)):
        writer(logminer_shell,sqlquery.format_text(add_log,archivelog[i]))
    writer(logminer_shell,sqlquery.format_text(logminer_shell_end,logminerdict,logconf['start_time'],logconf['end_time'],logminer_record,logconf['tablename'],logconf['schema']))