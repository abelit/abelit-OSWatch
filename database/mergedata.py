# encoding: utf-8
'''
@project:  __OSWATCH__
@modules:  database.mergedata
@description:
@created:  May 19, 2017

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''

from database import oracle

schema_name = ''
object_name = ''
dblink_name = ''
remote_schema_name = ''
remote_object_name = ''
condition = ''

def merge_data(schema_name,object_name,remote_schema_name,remote_object_name,dblink_name):    
    sql_merge = """
    MERGE INTO {0}.{1} dst USING
    ( SELECT
        t.*,
        NULL AS loc
    FROM
        {2}.{3} t {4}
    UNION ALL
    SELECT
        a.*,
        'deleteme' AS loc
    FROM
        {5}.{6} a
    LEFT JOIN
        {7}.{8} b
    ON a.bm = b.bm WHERE
       a.bm = '520100'
       AND
       b.bm IS NULL
    )
    src ON (
    dst.bm = src.bm
    ) WHEN MATCHED THEN
    UPDATE
    SET {}
    DELETE WHERE
    src.loc = 'deleteme'
    WHEN NOT MATCHED THEN INSERT ( {} ) VALUES ( {} )"""

    print(sql_merge)

if __name__ == '__main__':
    merge_data(schema_name, object_name, remote_schema_name, remote_object_name, dblink_name)
