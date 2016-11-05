#!/bin/sh
#Function: backup database with exp
#Usage: crontab on linux/Unix
#Last modfiy: 2015-09-07
#Set environment variable
ORACLE_BASE=/u01/app/oracle
ORACLE_HOME=/u01/app/oracle/product/11.2.0/db_1
ORACLE_SID=gzgszxk1
NLS_LANG=AMERICAN_AMERICA.ZHS16GBK
export ORACLE_BASE
export ORACLE_HOME
export ORACLE_SID
export NLS_LANG
#backup begin,exp_bak=/u01/app/oracle/exp_bak
FILE=`date +%Y%m%d%H%M`
echo "Exp Start Time: `date`  ">>/u01/app/oracle/exp_bak/exp.log 
/u01/app/oracle/product/11.2.0/db_1/bin/exp exp_user/oracle  owner=GZGS_HZ file=/u01/app/oracle/exp_bak/GZGS_HZ$FILE.dmp  log=/u01/app/oracle/exp_bak/GZGS_HZ$FILE.log 
echo "Exp Finish Time: `date`">>/u01/app/oracle/exp_bak/exp.log 

DUMP=/u01/app/oracle/exp_bak
export DUMP
find $DUMP -name "*.dmp" -mtime +2 -exec rm {} \;
find $DUMP -name "*.log" -mtime +2 -exec rm {} \;


