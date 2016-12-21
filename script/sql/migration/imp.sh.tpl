#!/bin/sh
#Function: restore database with imp
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

#The first parameter to asign the method of backup
PARA=$1
IMPUSER=backupuser/backuppassword

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

############################################################

#Store username from database
BACKUP_USERS=""
BACKUP_TABLES=""

#IMP Funciton
impByUser(){
    echo "Imp Start Time: `date` import by user OWNER: $OWNER">>$BACKUP_DIR/imp.log
    imp $IMPUSER fromuser=$OWNER touser=$OWNER  file=$BACKUP_DIR/$OWNER$FILE_NAME.dmp  log=$BACKUP_DIR/$OWNER$FILE_NAME.log 
    echo "Imp End Time: `date` import by user OWNER: $OWNER">>$BACKUP_DIR/imp.log 
}


#IMP Funciton
impByTable(){
    echo "Imp Start Time: `date` import by table TABLE: $TABLE">>$BACKUP_DIR/imp.log
    imp $IMPUSER  fromuser=$OWNER touser=$OWNER tables=$TABLE file=$BACKUP_DIR/$TABLE$FILE_NAME.dmp  log=$BACKUP_DIR/$TABLE$FILE_NAME.log 
    echo "Imp End Time: `date` import by table TABLE: $TABLE">>$BACKUP_DIR/imp.log 
}

#IMP Funciton
impByFull(){
    echo "Imp Start Time: `date` Import by full Full:$ORACLE_SID">>$BACKUP_DIR/imp.log
    imp $IMPUSER  full=y ignore=y file=$BACKUP_DIR/$ORACLE_SID$FILE_NAME.dmp  log=$BACKUP_DIR/$ORACLE_SID$FILE_NAME.log 
    echo "Imp End Time: `date` Import by full Full:$ORACLE_SID">>$BACKUP_DIR/imp.log 
}

############################################################

if [ "$PARA" = "byuser" ];then
    for OWNER  in $BACKUP_USERS
    do
        impByUser
    done
elif [ "PARA" = "bytable" ]; then
    for TABLE  in $BACKUP_TABLES
    do
        impByTable
    done
elif [ "PARA" = "byfull" ]; then
    impByFull
else
    echo "Warning: Please enter right parameter after $0 which like: $0 byuser|bytable|byfull"
fi


#Clean BACKUP_DIR
find $BACKUP_DIR -name "*.dmp" -mtime +2 -exec rm {} \;
find $BACKUP_DIR -name "*.log" -mtime +2 -exec rm {} \;