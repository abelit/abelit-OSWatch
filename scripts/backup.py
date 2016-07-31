#!/usr/bin/env python 
# -*- coding: utf-8 -*-
import time
import os
#配置oracle环境变量参数
os.environ["NLS_LANG"] = "Simplified Chinese_china.ZHS16GBK"
os.environ['ORACLE_HOME'] = '/u01/app/oracle/product/10.2.0/db_1'
os.environ['PATH'] = "/u01/app/oracle/product/10.2.0/db_1/bin" + ":" + os.environ['PATH']
os.environ['ORACLE_SID'] = "orcl"
os.environ['LANG'] = "en_US"
#打印提示信息。
exp_node1 = "\n成功备份文件到目录...."
exp_node2 = "备份失败"
cls_node1 = "数据库备份数据清除成功,删除语句如下...."
cls_node2 = "数据库备份清除失败"
#配置导出的数据库信息
data_path = '/tmp/'
db_username = 'abc'
db_password = 'abc'
db_name = 'abc'
"""
print os.getenv('NLS_LANG')
print os.getenv('ORACLE_HOME')
print os.getenv('ORACLE_SID')
"""
def oracle_exp():
    exp_command = "exp %s/%s file=%s%s_%s-%s-%s_%s%s%s.dmp log=%s%s_%s-%s-%s_%s%s%s.log"  % (db_username,db_password,data_path,db_name,time.strftime('%Y'),time.strftime('%m'),time.strftime('%d'),time.strftime('%H'),time.strftime('%M'),time.strftime('%S'),data_path,db_name,time.strftime('%Y'),time.strftime('%m'),time.strftime('%d'),time.strftime('%H'),time.strftime('%M'),time.strftime('%S'))
    exp_note = "数据库备份执行语句...."
    print "\033[1;31;40m%s\033[0m" %  exp_note
    print "\033[1;31;40m%s\033[0m" %  exp_command
    if os.system(exp_command) == 0:
        print "\033[1;32;40m%s\033[0m" % exp_node1
    else:
        print "\033[1;31;40m%s\033[0m" % exp_node2
def backup_clear():
    cls_command1 = '/usr/bin/find %s -mtime +1 -name "*.dmp" -exec rm -rf {} \;' % data_path
    cls_command2 = '/usr/bin/find %s -mtime +1 -name "*.log" -exec rm -rf {} \;' % data_path
    if os.system(cls_command1) == 0 and os.system(cls_command2) == 0:
        print "\n\033[1;32;40m%s\033[0m" % cls_node1
    else:
        print "\n\033[1;31;40m%s\033[0m" % cls_node2
    print "%s" % cls_command1
    print "%s\n" % cls_command2
def main():
    oracle_exp()
    backup_clear()
if __name__ == "__main__":
    main()
