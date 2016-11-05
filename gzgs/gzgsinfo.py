# encoding: utf-8
'''
@project: __oswatch__
@modules: gzgs.gzgsinfo
@description:
    
@created:Sep 19, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

from database.datasync import DataSync
from database import oracle
from gzgs import gzgsconf

# Configuration
tablesrc = ['A_BM_XZQH']
tabledst = ['A_BM_XZQH']
ownersrc = ['GZGS_GY']
ownerdst = ['GZGS_HZ']
condition = ""
method = 'merge'


class GZGSInfo:
    """docstring for GZGS"""
    def sync_data(self, plan='manual',
                  method=method,
                  tablesrc=tablesrc,
                  tabledst=tabledst,
                  ownersrc=ownersrc,
                  ownerdst=ownerdst,
                  condition=condition):
        """If plan is all meaning that sync all all tables under the
         user bellow ownersrc list to ownerdst list"""
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
                tablesrc = oracle.SQLQuery().query_sql(sql, {'owner':i})
            for j in tablesrc:
                for k in ownerdst:
                    if plan == 'all':
                        tabledst = oracle.SQLQuery().query_sql(sql, {'owner':k})
                    for m in tabledst:
                        DataSync().sync_data(
                            method=method,
                            tablesrc=j,
                            tabledst=m,
                            ownersrc=i,
                            ownerdst=k,
                            condition=condition)
if __name__ == '__main__':
    GZGSInfo().sync_data()
