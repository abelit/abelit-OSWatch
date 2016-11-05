#!/bin/sh
#Function: backup database with expdp
#Usage: crontab on linux/unix
#Last modfiy: 2016-06-04

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

#Set directories of exp's backup
EXP_BACKUP_DIR=/u01/app/oracle/exp_backup
if [ !-d "$EXP_BACKUP_DIR"  ];then
    mkdir -p $EXP_BACKUP_DIR
fi
# Set time format for the filename
FILE_NAME=`date +%Y%m%d%H%M`
# Get parameter from user's input
PARAMS=$1

# GZGS_HZ
GZGS_HZ_SCHEMA="GZGS_HZ"

# The schema of 13 States
GZGS_SCHEMA="
GZGS_GY
GZGS_ZY
GZGS_AS
GZGS_LPS
GZGS_TR
GZGS_BJ
GZGS_QN
GZGS_QDN
GZGS_QXN
GZGS_GA
GZGS_WLX
GZGS_RHX
GZGS_SGS"

GZGSNJ_SCHEMA="
GZGSNJ_GY
GZGSNJ_ZY
GZGSNJ_AS
GZGSNJ_LPS
GZGSNJ_TR
GZGSNJ_BJ
GZGSNJ_QN
GZGSNJ_QDN
GZGSNJ_QXN
GZGSNJ_GA
GZGSNJ_WLX
GZGSNJ_RHX
GZGSNJ_SGS"

GZGS_12315_SCHEMA="
GZGS_12315
GZGS_12315DC
GZGS_12315WW
GZGS_12315ZSK"

GZGS_WSBSDT_SCHEMA="
GZGS_WSBSDT
GZGS_WSBSDT_DISPATCH"

GZGS_CREDIT_SCHEMA="
CREDIT
GZGS_CREDIT
GZGS_CREDIT_CESHI
GZGS_CREDIT_EXT
GZGS_CREDIT_NEW"

GZGS_BAOBIAO_SCHEMA="GZGS_BAOBIAO"

#GZGS_LINSHI_SCHEMA="GZGS_LINSHI"

GZGS_OTHER_SCHEMA="
GZGS_ZTK
GZGS_WXQY
GZGS_QINGXI
GZGS_M
DBTURN_GZGS
GSSC_INSPUR_IN
GZCREDIT
GZGS_TEST
GZGS_TEST01
GZGS_XYFLJG
GZGS
GZGSCW_ZCGL
GZGSXY
GZGS_AJCX
GZGS_AJUSER
GZGS_GA01
GZGS_GZCREDIT
GZGS_HZ_NZSC
GZGS_LINKAGE_HZ
GZGS_LINKAGE_LPS
ZZ"

#EXP funciton
exp_byuser(){
    echo "Exp Start Time: `date` OWNER: $OWNER">>$EXP_BACKUP_DIR/exp.log
    exp exp_user/oracle  owner=$OWNER file=$EXP_BACKUP_DIR/$OWNER$FILE_NAME.dmp  log=$EXP_BACKUP_DIR/$OWNER$FILE_NAME.log
    echo "Exp Finish Time: `date` OWNER: $OWNER">>$EXP_BACKUP_DIR/exp.log
}

case $PARAMS in
    GZGS_HZ_SCHEMA)  SCHEMAS=$GZGS_HZ_SCHEMA
    ;;
    GZGS_SCHEMA)  SCHEMAS=$GZGS_SCHEMA
    ;;
    GZGSNJ_SCHEMA)  SCHEMAS=$GZGSNJ_SCHEMA
    ;;
    GZGS_12315_SCHEMA)  SCHEMAS=$GZGS_12315_SCHEMA
    ;;
    GZGS_WSBSDT_SCHEMA)  SCHEMAS=$GZGS_WSBSDT_SCHEMA
    ;;
    GZGS_CREDIT_SCHEMA)  SCHEMAS=$GZGS_CREDIT_SCHEMA
    ;;
    GZGS_BAOBIAO_SCHEMA)  SCHEMAS=$GZGS_BAOBIAO_SCHEMA
    ;;
    GZGS_OTHER_SCHEMA)  SCHEMAS=$GZGS_OTHER_SCHEMA
    ;;
    GZGS_OTHER_SCHEMA)  SCHEMAS=$GZGS_OTHER_SCHEMA
    ;;
    *)  SCHEMAS=$PARAMS
    ;;
esac

# Call exp_byuser to start exp the schema
if [ "$SCHEMAS" != "" ];then
    for OWNER in $SCHEMAS
    do
        exp_byuser
    done
fi
