#!/bin/sh
#Function: backup database with exp
#Usage: crontab on linux/Unix
#Last modfiy: 2016-07-28

#Set environment variable
export ORACLE_BASE=/u01/app/oracle
export ORACLE_HOME=/u01/app/oracle/product/11.2.0/db_1
export ORACLE_SID=gzgszxk1
export NLS_LANG=AMERICAN_AMERICA.ZHS16GBK
export ORACLE_BIN=$ORACLE_HOME/bin/
export PATH=$PATH:ORACLE_BIN

#Define variable and parameter
export BACKUP_DIR=/u01/app/oracle/backup
export FILE_NAME=`date +%Y%m%d%H%M`
PARA=$1
EXPDPUSER=backupuser/backuppassword

#######################################################################3

#Check backup_dir
if [ ! -d $BACKUP_DIR ];then
    echo "INFO: Now creating direcroty $BACKUP_DIR"
    mkdir -m 755 -p $BACKUP_DIR
    if [ -d $BACKUP_DIR ];then
        echo "INFO: Direcroty $BACKUP_DIR has been created succefully!"
    else
        echo "Warning: Fail to create direcroty $BACKUP_DIR! Please create the dir by manual!"
    fi
fi


#Store username from database
BACKUP_USERS=""
BACKUP_TABLES=""
BACKUP_TABLESPACES=""

#EXPDP Funciton
expdpByUser(){
    echo "Expdp Start Time: `date` export by user OWNER: $OWNER">>$BACKUP_DIR/expdp.log
    expdp $EXPDPUSER schemas=$OWNER directory=BACKUP dumpfile=$OWNER$FILE_NAME_%U.dump logfile=$OWNER$FILE_NAME.log  parallel=16 cluster=n job_name=$ORACLE_SID$FILE_NAME REUSE_DUMPFILES=Y 
    echo "Expdp End Time: `date` export by user OWNER: $OWNER">>$BACKUP_DIR/expdp.log 
}


#EXPDP Funciton
expdpByTable(){
    echo "Expdp Start Time: `date` export by table TABLES:$TABLE">>$BACKUP_DIR/expdp.log
    expdp $EXPDPUSER tables=$TABLE directory=BACKUP dumpfile=$TABLE$FILE_NAME_%U.dump logfile=$TABLE$FILE_NAME.log parallel=16 cluster=n job_name=$ORACLE_SID$FILE_NAME REUSE_DUMPFILES=Y  
    echo "Expdp End Time: `date` export by table TABLES:$TABLE">>$BACKUP_DIR/expdp.log 
}

#EXPDP Funciton
expdpByTablespace(){
    echo "Expdp Start Time: `date` export by tablespace TABLESPACE:$TABLESACE">>$BACKUP_DIR/expdp.log
    expdp $EXPDPUSER tablespaces=$TABLESACE directory=BACKUP dumpfile=$TABLESPACE$FILE_NAME_%U.dump logfile=$TABLESPACE$FILE_NAME.log  parallel=16 cluster=n job_name=$ORACLE_SID$FILE_NAME REUSE_DUMPFILES=Y   
    echo "Expdp End Time: `date` export by tablespace TABLESPACE:$TABLESACE">>$BACKUP_DIR/expdp.log 
}

#EXPDP Funciton
expdpByFull(){
    echo "Expdp Start Time: `date` export by full Full: $ORACLE_SID">>$BACKUP_DIR/expdp.log
    expdp $EXPDPUSER  full=y directory=BACKUP dumpfile=$ORACLE_SID$FILE_NAME_%U.dump logfile=$ORACLE_SID$FILE_NAME.log  parallel=16 cluster=n job_name=$ORACLE_SID$FILE_NAME REUSE_DUMPFILES=Y
    echo "Expdp End Time: `date` export by full Full: $ORACLE_SID">>$BACKUP_DIR/expdp.log 
}

####################################################################

if [ "$PARA" = "byuser" ];then
    for OWNER  in $BACKUP_USERS
    do
        expdpByUser
    done
elif [ "PARA" = "bytable" ]; then
    for TABLE  in $BACKUP_TABLES
    do
        expdpByTable
    done
elif [ "$PARA" = "bytablespace"];then
    for TABLESPACE in $BACKUP_TABLESPACES
    do
        expdpByTablespace
    done
elif [ "PARA" = "byfull" ]; then
    expdpByFull
else
    echo "Warning: Please enter right parameter after $0 which like: $0 byuser|bytable|bytablespace|byfull"
fi

#######################################################################

#Clean BACKUP_DIR
find $BACKUP_DIR -name "*.dmp" -mtime +2 -exec rm {} \;
find $BACKUP_DIR -name "*.log" -mtime +2 -exec rm {} \;