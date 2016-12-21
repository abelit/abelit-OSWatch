#!/bin/sh
#Function: backup database with expdp
#Usage: crontab on linux/Unix
#Last modfiy: 2016-04-19

#Set environment variable
ORACLE_BASE=/u01/app/oracle
ORACLE_HOME=/u01/app/oracle/product/11.2.0/db_1
ORACLE_SID=gzgszxk1
NLS_LANG=AMERICAN_AMERICA.ZHS16GBK
ORACLE_BIN=$ORACLE_HOME/bin/
export ORACLE_BASE
export ORACLE_HOME
export ORACLE_SID
export NLS_LANG
export ORACLE_BIN
export PATH=$PATH:ORACLE_BIN

#Define variable and parameter
LOG_DIR=/u01/app/oracle/expdpbak
DUMP_DIR=$LOG_DIR
FILE_NAME=`date +%Y%m%d%H%M`
PARA=$1
ALL_USERS="GZGS_WSBSDT_DISPATCH GZGS_WXQY GZGS_WSBSDT_DISPATCH GZGS_12315ZSK GZGS_12315WW GZGS_12315
GZGS_WLX GZGS_RHX GZGSXY GZGS_WSBSDT GZGS_LINKAGE_LPS GZGS_LINKAGE_HZ GZGS_CREDIT_EXT
GZGS_BAOBIAO GZGS_XYFLJG GZGS_GZCREDIT GZGS_CREDIT GZGS_HZ_NZSC GSSC_INSPUR_IN
GZGS_GA01 GZGS_12315DC GZGSNJ_HZ GZGSNJ_ZY GZGSNJ_TR GZGSNJ_QXN GZGSNJ_QN GZGSNJ_QDN GZGSNJ_GY
GZGSNJ_LPS GZGSNJ_BJ GZGSNJ_AS GZGSNJ_SGS GZGS_AJUSER GZGS_ZTK GZGS_M GZGS_AJCX 
GZGS_GA GZGS_LINSHI DBTURN_GZGS GZGS_QINGXI GZGS_QN GZGS_QDN GZGS_BJ GZGS_QXN GZGS_TR GZGS_AS GZGS_LPS
GZGS_ZY GZGS_GY GZGS_SGS GZGSCW_ZCGL GZCREDIT CREDIT GZGS GZGS_CREDIT_NEW"

#expdp funciton
expdpFunction(){
    echo "Expdp Start Time: `date` OWNER: $OWNER">>$LOG_DIR/expdp.log
    expdp \'abelit\/cy123\' directory=EXPDP_BAK dumpfile=${OWNER}_%U.dump logfile=$OWNER.log schemas=$OWNER trace=480300 parallel=16 cluster=n 
    job_name=$ORACLE_SID$FILE_NAME
    echo "Expdp Finish Time: `date` OWNER: $OWNER">>$LOG_DIR/expdp.log 
}

if [ "$PARA" = "all" ];then
    ALL_USERS="$ALL_USERS GZGS_HZ"
    for OWNER  in $ALL_USERS
    do
        expdpFunction
    done
elif [ "$PARA" = "other"  ];then
    for OWNER in $ALL_USERS
    do
        expdpFunction
    done
else
    OWNER=$PARA
    expdpFunction
fi
