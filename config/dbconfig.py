# encoding: utf-8
'''
@project: __oswatch__
@modules: config.dbconfig
@description:
@created: Jul 31, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

import os

"""Import customized modules"""
from oswatch import pathget
"""
project is dict for configuring the basic infomation of this project
"""
project={
    'project_name':'__oswatch__.py',
    'author':'abelit',
    'email':'ychenid@live.com',
    'description':''
}

# Oracle configuration
oracle={
    #Oracle DB configuration
    'username':'sys',
    'password':'dba1d71f678513c02d0',
    'host':'172.28.1.221',
    'port':'1521',
    'instance':'gzgszxk',
    #Oracle env variable
    'ORACLE_BASE':'/u01/app/oracle',
    'ORACLE_HOME':'/u01/app/oracle/product/11.2.0/db_1',
    'ORACLE_SID':'gzgszxk2',
    'NLS_DATE_FORMAT':'yyyy-mm-dd HH24:MI:SS',
    'NLS_LANG':'AMERICAN_AMERICA.ZHS16GBK',
    # Oracle backup info
    'BACKUP_DIR':'/u01/app/oracle/backup'
}

"""
log is dict for configuring parameters of logging
logpath: path or file using to store program running logs
logconf: path or file using to config logging's configuration
"""
# Get the separater of os
separater=os.sep

# Set the path of project 
project_path=pathget.get_filepath(project['project_name'])

log={
    'logpath':project_path+separater+'log'+separater+'dblog.log',
    'logconf':project_path+separater+'config'+separater+'logging.conf'
}

script={
    'scriptpath':project_path + '/scripts'
}



if __name__ == '__main__':
    print(log['logpath'])
    
    