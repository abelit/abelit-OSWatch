# encoding: utf-8
'''
@project: __oswatch__
@modules: database.datasync
@description:
@created: Jul 31, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

import cx_Oracle  # @UnresolvedImport

"""Import customized modules"""
from database import oracle
from oswatch import logwrite

class DataSync(object):
    def sync_data(self, method, tablesrc, tabledst, ownersrc, ownerdst, condition):
        '''
        Function: sync_data
        Summary: sync_data is a method belongs to class DataSync to synchronize data between two tables
        Examples:   tablesrc = 'A_BM_XZQH'
                    tabledst = 'A_BM_XZQH'
                    ownersrc = 'GZGS_GY'
                    ownerdst = 'GZGS_HZ'
                    condition = ""
                    method = 'merge'
                    DataSync().sync_data(method=method, tablesrc=tablesrc, tabledst=tabledst, \
                        ownersrc=ownersrc, ownerdst=ownerdst, condition=condition)
        Attributes: 
            @param (self):InsertHere
            @param (method):It's a synchronous method including "merge" and "delete,insert"
            @param (tablesrc):The source table
            @param (tabledst):The target table
            @param (ownersrc):The source table of owner
            @param (ownerdst):The target table of owner
            @param (condition):The condition in the sql like "where ..."
        Returns: 0
        '''
        tablesrc_pk = oracle.Table().primarykey(tablesrc, ownersrc)
        tablesrc_field = oracle.Table().field(tablesrc, ownersrc)
        tabledst_pk = oracle.Table().primarykey(tabledst, ownerdst)
        tabledst_field = oracle.Table().field(tabledst, ownerdst)
       
        issync = True
        loglevel = "infoLogger"
        logmessage = ""
        
        # Example merge sql to sychrorize data
        sql_merge = """MERGE INTO {0} dst USING {1} src ON ({2} = {3})
        WHEN MATCHED THEN
        UPDATE SET {4} {5} 
        WHEN NOT MATCHED THEN 
        INSERT VALUES({6}) {7}"""
        # Example sql to query diffrent data between two tables
        sql_diff ="select {0} from (select * from {1} {2} minus select * from {3} {4})"
        # Example sql to delte data
        sql_delete = "delete from {0} where {1} in (select {2} from (select * from {3} {4} \
            minus select * from {5} {6}))"
        # Example sql to insert data
        sql_insert = "insert into {0} (select * from {1} {2} minus select * from {3} {4})"
 
        # The two tables should have the same field name
        if tablesrc_field  != tabledst_field:
            issync = False
            loglevel = "errorLogger"
            logmessage = "There are diffrences between two tables ("+tablesrc+","+tabledst+\
                ") which need to be synchorizing."

        

        # The two tables should have the same primary key
        if len(tablesrc_pk) and len(tabledst_pk) and tablesrc_pk == tabledst_pk:
            issync = True   
        else:
            issync = False
            loglevel = "errorLogger"
            logmessage="Empty list, no primary key on table " + ownersrc + '.' + tablesrc + \
                ' or ' + ownersrc + '.' + tablesrc


        if issync:
            # Query the diffrence data between two table
            results = oracle.Oracle().select(sql_diff.format('*',ownersrc + '.' + tablesrc, \
                condition, ownerdst + '.' + tabledst, condition))
            # Generate  for condition strings
            strings = []
            [strings.append(results[i][0]) for i in range(len(results))]
            condition = condition + ' WHERE ' + tablesrc_pk[0] + ' IN ' + str(tuple(strings))

        if results and method == 'merge':                  
            # Using merge sync data
            
            # update_fields = ''
            # for row in range(1, len(tablesrc_field) - 1):
            #     update_fields = update_fields + 'dst.' + tablesrc_field[row] + '=' + \
            #         'src.' + tablesrc_field[row] + ','
            # update_fields = update_fields + 'dst.' + tablesrc_field[len(tablesrc_field) - 1] + \
            #     '=' + 'src.' + tablesrc_field[len(tablesrc_field) - 1]
            #     
            # insert_fields = ''
            # for row in range(len(tablesrc_field) - 1):
            #     insert_fields = insert_fields + 'src.' + tablesrc_field[row] + ','
            # insert_fields = insert_fields + 'src.' + tablesrc_field[len(tablesrc_field) - 1]
            
            # Join string like values(field1,field2,...)
            update_strings = []
            tabledst_field.remove(tabledst_pk) # update all fields but primary key field,so remove it from list
            [update_strings.append('dst.'+i+'='+'src.'+i) for i in tabledst_field]
            update_fields = ",".join(update_strings)

            insert_strings = []
            [insert_strings.append('src.'+i) for i in tabledst_field]
            insert_fields = ",".join(insert_strings)

            sql_merge = sql_merge.format(ownerdst + '.' + tabledst, ownersrc + '.' + tablesrc, \
                'dst.' + tablesrc_pk[0], 'src.' + tablesrc_pk[0], update_fields, condition, \
                insert_fields, condition)
            
            try:
                # Call modules to excute sql
                oracle.Oracle().execute(sql_merge)
            except cx_Oracle.DatabaseError:
                logmessage="Sync data error"
                loglevel='errorLogger'
            else:
                logmessage = 'The diffrent data: ' + results + \
                    '\nThe sql to sync data: ' + sql_merge + \
                    "\nSync data successfully between " + ownersrc + '.' + tablesrc + \
                    ' and ' + ownerdst + '.' + tabledst
                loglevel='infoLogger'
        # Using minus,delete,insert sync data
        elif results and method == 'insert':
            sql_delete = sql_delete.format(\
                ownerdst + '.' + tabledst, tabledst_pk[0], tabledst_pk[0], \
                ownersrc + '.' + tablesrc, condition, ownerdst + '.' + tabledst, condition)
            sql_insert = sql_insert.format(\
                ownerdst + '.' + tabledst, ownersrc + '.' + \
                tablesrc, condition, ownerdst + '.' + tabledst, condition)
            try:
                # delete data
                oracle.Oracle().execute(sql_delete)
                # insert data
                oracle.Oracle().execute(sql_insert)
            except cx_Oracle.DatabaseError:
                logmessage = "Sync data error"
                loglevel = 'errorLogger'
            else:
                logmessage ='The diffrent data: ' + results + \
                    '\n The sql to sync data: ' + sql_delete + sql_insert+ \
                    "\nSync data successfully between " + ownersrc + '.' + tablesrc + \
                    ' and ' + ownerdst + '.' + tabledst
                loglevel = 'infoLogger'
        elif results in None: 
            #results is a variable defined before which returns the diffrent data between tow tables
            logmessage = "No data need to syncthonize between " + ownersrc + '.' + tablesrc + \
                ' and ' + ownerdst + '.' + tabledst
            loglevel = 'warnLogger'
        else:
            logmessage = "Please input 'insert' or 'merge' to sync data,like syncdata(merge)"
            loglevel = 'errorLogger'

        # Call logwrite modules to write log
        logwrite.LogWrite(loglevel=loglevel, logmessage=logmessage).write_log()

class DataCompare(object):
    """class DataCompare Doc"""
    def compare_data(self):
        # Example sql to query diffrent data between two tables
        sql_diff = "select {0} from (select * from {1} {2} minus select * from {3} {4})"
        # Query the diffrence data between two table
        results = oracle.Oracle().select(sql_diff.format('*',ownersrc + '.' + \
            tablesrc, condition, ownerdst + '.' + tabledst, condition))


if __name__ == '__main__':
    tablesrc = 'A_BM_XZQH'
    tabledst = 'A_BM_XZQH'
    ownersrc = 'GZGS_GY'
    ownerdst = 'GZGS_HZ'
    condition = ""
    method = 'merge'
    DataSync().sync_data(method=method, tablesrc=tablesrc, tabledst=tabledst, \
        ownersrc=ownersrc, ownerdst=ownerdst, condition=condition)
