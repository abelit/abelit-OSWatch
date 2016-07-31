#!/bin/sh
export ORACLE_SID=gzgszxk1

#方法一：
echo "
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
execute dbms_logmnr.add_logfile(LogFileName=>'+ARCHIVELOG/gzgszxk/archivelog/2016_07_07/thread_1_seq_125.30231.916531401',Options=>dbms_logmnr.new);
execute dbms_logmnr.add_logfile(LogFileName=>'+ARCHIVELOG/gzgszxk/archivelog/2016_07_07/thread_2_seq_123.30226.916531411',Options=>dbms_logmnr.addfile);
execute dbms_logmnr.start_logmnr(DictFileName=>'/u01/app/oracle/logminer/dictionary.ora');
spool /u01/app/oracle/logminer/record.txt;
select to_clob(sql_redo)||'|'||to_clob(sql_undo)||'|'||to_char(scn)||'|'||to_char(timestamp)||'|'||to_char(session_info)||'|'||to_char(table_name)||'|'||to_char(seg_owner)||'?'
from v\$logmnr_contents WHERE seg_name='A_BM_XZQH'AND seg_owner='GZGS_GY';
spool off;
" | sqlplus '/as sysdba'>/dev/null

#方法二
/*sqlplus / as sysdba << !
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
execute dbms_logmnr.add_logfile(LogFileName=>'+ARCHIVELOG/gzgszxk/archivelog/2016_07_07/thread_1_seq_125.30231.916531401',Options=>dbms_logmnr.new);
execute dbms_logmnr.add_logfile(LogFileName=>'+ARCHIVELOG/gzgszxk/archivelog/2016_07_07/thread_2_seq_123.30226.916531411',Options=>dbms_logmnr.addfile);
execute dbms_logmnr.start_logmnr(DictFileName=>'/u01/app/oracle/logminer/dictionary.ora');
spool /u01/app/oracle/logminer/record.txt;
select to_clob(sql_redo)||'|'||to_clob(sql_undo)||'|'||to_char(scn)||'|'||to_char(timestamp)||'|'||to_char(session_info)||'|'||to_char(table_name)||'|'||to_char(seg_owner)||'?'
from v\$logmnr_contents WHERE seg_name='A_BM_XZQH'AND seg_owner='GZGS_GY';
spool off;
exit
!
*/
