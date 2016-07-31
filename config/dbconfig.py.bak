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


import time
import getpass
import os
"""
Import customized modules
"""
from core import pathget

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
    'host':'172.28.1.222',
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
log={
    'logpath':pathget.PathGet(project['project_name']).get_fullpath() + '/logs/dblog.log',
    'logconf':pathget.PathGet(project['project_name']).get_fullpath()+'/config/logging.conf'
}

script={
    'scriptpath':pathget.PathGet(project['project_name']).get_fullpath() + '/scripts'
}
