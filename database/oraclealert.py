#!/usr/bin/env python
#coding:gbk
"""
  Author:  Edward.Zhou --- <edgeman_03@163.com>
  Purpose: Oracle数据库alert_sid.log日志信息巡检
  Created: 2016/5/21
"""
 
import socket
import ftplib
import re
import os
import codecs
from datetime import date
 
 
#下载模块
def ftpget(host, ftpuser, ftppasswd, ftpfile):
    ftpcnt = 0
    ftpstat = []
    errlogs = {}
    try:
        ftp = ftplib.FTP(host)
        ftp.set_debuglevel(0)
    except (socket.error, socket.gaierror):
        ftpcnt = 1
        err = "ERROR:Can't reach %s" % host
        ftpstat.append(ftpcnt)
        ftpstat.append(err)
        return ftpstat
 
    try:
        ftp.login(user= ftpuser, passwd= ftppasswd)
    except ftplib.error_perm:
        ftpcnt = 1
        err = "ERROR: cannot login,Please check user and password."
        ftpstat.append(ftpcnt)
        ftpstat.append(err)
        ftp.quit()
        return ftpstat
     
    #print ftp.getwelcome()
     
    try:
        ftp.cwd("ORA_ALERT_LOG")
    except ftplib.Error:
        ftpcnt = 1
        err = "FTP SERVER not change 'ORA_ALERT_LOG' directory."
        ftpstat.append(ftpcnt)
        ftpstat.append(err)
        ftp.quit()
        return ftpstat
     
    if ftpfile in ftp.nlst():
        path = "C:\\Windows\\Temp\\"+ftpfile
        f = open(path, 'wb')
        ftp.retrbinary("RETR " + ftpfile, f.write, 1024)
        ftpcnt = 0
        ftpstat.append(ftpcnt)
        ftpstat.append(path)
        ftp.quit()
        return ftpstat
    else:
        ftpcnt = 1
        err = "Need to download the file %s does not exist." % (ftpfile)
        ftpstat.append(ftpcnt)
        ftpstat.append(err)
        ftp.quit()
        return ftpstat
 
    ftp.set_debuglevel(0)
    ftp.close()
 
     
#ALERT_LOG日志查找告警信息
def logsearch(filename):
    cnt = 0
    logstat = []
    lines = open(filename, "r").read().decode("utf-8")
    rs = re.search(r'ORA-.*', lines)
    if rs:
        cnt = 1
        errlog = rs.group()
        logstat.append(cnt)
        logstat.append(errlog)
    else:
        logstat.append(cnt)
 
    return logstat
 
 
 
def main():
    ftpfilelist = {
        "yj-sop": "10.44.98.12alert_orcl.log",
        "yj-cba": "10.44.98.101alert_orcl.log",
        "yj-zhdb": "10.44.98.10alert_yjdb02.log",
        "yj-vib": "10.44.80.18alert_orcl.log",
        "ycomis": "10.44.98.20alert_orcl.log",
        "yj-db": "10.44.98.92alert_orcl.log"
    }
 
    host = '192.168.223.1'
    ftpuser = 'backup'
    ftppasswd = 'backup'
     
 
    print "日期：%s 星期：%s"%(date.today(),date.isoweekday(date.today()))
    print
    print "Oracle数据库Alert log日志警告信息巡检："
         
    for hostname in ftpfilelist:
        logmsg = ftpget(host, ftpuser, ftppasswd, ftpfilelist[hostname])
        if logmsg[0] == 1:
            print "服务器：%s\t日志文件：%s\t状态：%s\t错误：%s" % (hostname, ftpfilelist[hostname], "异常", logmsg[1])
        else:
            errlogs = logsearch(logmsg[1])
            os.remove(logmsg[1])
            if errlogs[0] == 0:
                print "服务器：%s\t日志文件：%s\t状态：%s" % (hostname, ftpfilelist[hostname], "正常")
            else:
                print "服务器：%s\t日志文件：%s\t状态：%s\t错误：%s" % (hostname, ftpfilelist[hostname], "异常", (errlogs[1]).encode("gbk"))
 
                 
if __name__ == '__main__':
    main()