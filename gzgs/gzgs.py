#!/usr/bin/python
#coding:utf-8

# Function:Synchronize data between two tables
# Description:
# Usage:
# Function gzgsdata
# Author:Abelit
# Email:ychenid@live.com
# Date:2016-07-01

import __init__
from database.datasync import DataSync
from database.db import Oracle
import gzgsconf

# Configuration
tablesrc = ['A_BM_XZQH']
tabledst = ['A_BM_XZQH']
ownersrc = ['GZGS_GY']
ownerdst = ['GZGS_HZ']
condition = ""
method = 'merge'


class GZGS:
    """docstring for GZGS"""

    def __init__(self, arg):
        super(GZGS, self).__init__()
        self.arg = arg

    def sync_data(plan='manual',
                  method=method,
                  tablesrc=tablesrc,
                  tabledst=tabledst,
                  ownersrc=ownersrc,
                  ownerdst = ownerdst,
                  condition=condition):
        #If plan is all meaning that sync all all tables under the user bellow ownersrc list to ownerdst list
        if plan == 'all':
            ownersrc = ['GZGS_GY', 'GZGS_ZY', 'GZGS_BJ', 'GZGS_RHX',
                        'GZGS_LPS', 'GZGS_WLX', 'GZGS_TR', 'GZGS_QN',
                        'GZGS_QXN', 'GZGS_QDN', 'GZGS_AS', 'GZGS_GA',
                        'GZGS_SGS']
            ownerdst = ['GZGS_HZ']
            sql = '''
            select table_name from all_tables where owner=:owner
            '''
        for i in ownersrc:
            if plan == 'all':
                tablesrc = Oracle.SQLQuery().query_sql(sql,{'owner':i})
            for j in tablesrc:
                for k in ownerdst:
                    if plan == 'all':
                        tabledst = Oracle.SQLQuery().query_sql(sql,{'owner':k})
                    for m in tabledst:
                        DataSync.sync_data(
                            method=method,
                            tablesrc=j,
                            tabledst=m,
                            ownersrc=i,
                            ownerdst=k,
                            condition=condition)


if __name__ == '__main__':
    GZGS.sync_data()