# encoding: utf-8
'''
@project: __oswatch__
@modules: config.dbconfig
@description: This is a default configuration of the project, 
    and user can define his or her own configuration under config dirrecory
@created: Jul 31, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

import os
from oswatch import pathget
import configparser

package = "dev"
version = "3.14"

# project is dict for configuring the basic infomation of this project
project = {
    'project_name':'__oswatch__.py',
    'author':'abelit',
    'email':'ychenid@live.com',
    'description':''
}

# Get the separater of os and set the base path of project 
separater = os.sep
project_path = pathget.get_fullpath(project['project_name'])
config_path = {
    'dbconf':project_path + separater + 'config' + separater + 'database.conf',
    'serverconf':project_path + separater + 'config' + separater + 'server.conf',
    'reportconf':project_path + separater + 'config' + separater + 'report.conf',
    'logconf':project_path + separater + 'config' + separater + 'logging.conf'
}

# Get the configuration of user-defined
# Get the configuration of database
dbconf = configparser.ConfigParser()
dbconf.read(config_path['dbconf'])

# Oracle configuration
oracle = {
    # Oracle DB configuration
    'username':dbconf.get('oracle', 'username'),
    'password':dbconf.get('oracle', 'password'),
    'host':dbconf.get('oracle', 'host'),
    'port':dbconf.getint('oracle', 'port'),
    'instance':dbconf.get('oracle', 'instance'),
    
    # Oracle env variable
    'ORACLE_BASE':'/u01/app/oracle',
    'ORACLE_HOME':'/u01/app/oracle/product/11.2.0/db_1',
    'ORACLE_SID':'gzgszxk2',
    'NLS_DATE_FORMAT':'yyyy-mm-dd HH24:MI:SS',
    'NLS_LANG':'AMERICAN_AMERICA.ZHS16GBK',
    
    # Oracle backup info
    'BACKUP_DIR':'/u01/app/oracle/backup'
}

# log is dict for configuring parameters of logging
# logpath: path or file using to store program running logs
log = {
    'logpath':project_path + separater + 'log' + separater + project['project_name']+'.log'
}

# Store sql or shell scripts
script = {
    'scriptpath':project_path + os.sep +'script'
}

# Assign the directory for storing data pump
backup = {
    'exp': {
        'backup_user':'exp_user/gzgsoracle',
        'exp_type':'byuser',
        'exp_parameter':'',
        'exp_path':'/u01/app/oracle/exp_backup',
        'exp_table':'a_qyzt',
        'exp_user':'gzgs_hz'
    },
    'expdp': {
        'backup_user':'exp_user/gzgsoracle',
        'expdp_type':'byschema',
        'expdp_parameter':'',
        'expdp_path':'expdp_backup',
        'expdp_table':'a_qyzt',
        'expdp_schema':'gzgs_hz',
        'expdp_tablespace':'gzgs_hz'
    },
    'imp': {
        'backup_user':'exp_user/gzgsoracle',
        'imp_type':'byuser',
        'imp_parameter':'',
        'imp_path':'/u01/app/oracle/exp_backup',
        'imp_table':'a_qyzt',
        'from_user':'gzgs_hz',
        'to_user':'gzgs_hz',

    },
    'impdp': {
        'backup_user':'exp_user/gzgsoracle',
        'impdp_type':'byschema',
        'impdp_parameter':'',
        'impdp_path':'expdp_backup',
        'impdp_table':'a_qyzt',
        'from_schema':'gzgs_hz',
        'to_schema':'gzgs_hz',
        'from_tablespace':'gzgs_hz',
        'to_tablespace':'gzgs_hz'
    }
}


if __name__ == '__main__':
    print(log['logpath'])
    print(oracle['username'])
    print(oracle['password'])
    print(oracle['host']+":"+str(oracle['port']))
    
