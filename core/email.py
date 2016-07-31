[python] 自动发邮件 增加日志 (2011-08-10 15:04:50)转载▼
标签： 杂谈  分类： 脚本
# -*- coding: cp936 -*-
##
#toaddr = 'buffy@sunnydale.k12.ca.us'
#cc = ['alexander@sunydale.k12.ca.us','willow@sunnydale.k12.ca.us']
#bcc = ['chairman@slayerscouncil.uk']
#fromaddr = 'giles@sunnydale.k12.ca.us'
#message_subject = "disturbance in sector 7"
#message_text = "Three are dead in an attack in the sewers below sector 7."
#message = "Subject %s\r\n" % message_subject
 
        #+"From: %s\r\n" % fromaddr
         #+"To: %s\r\n" % toaddr
         #+"CC: %s\r\n" % ",".join(cc)
         #+"Subject: %s\r\n" % message_subject
         #+"\r\n" + message_text
#toaddrs = [toaddr] + cc + bcc
#server = smtplib.SMTP('smtp.sunnydale.k12.ca.us')
#server.set_debuglevel(1)
#server.sendmail(fromaddr, toaddrs, message)
#server.quit()


import smtplib
from email.mime.text import MIMEText
from email.MIMEMultipart import MIMEMultipart
import datetime
import time
import logging
import os

logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S',
        filename='d:/report/mail.log',
        filemode='w')

TODAY = datetime.date.today()
CURRENTDAY=TODAY.strftime('%Y%m%d')
def sendattachmail ():
    msg = MIMEMultipart()
    att = MIMEText(open(r'D:\report\aa.xlsx', 'rb').read(), 'base64', 'gb2312') #设置附件的目录
    att['content-type'] = 'application/octet-stream'
    att['content-disposition'] = 'attachment;filename="IMD_EBM.xlsx"' #设置附件的名称
    msg.attach(att)

    content = '附件请查收。' #正文内容
    body = MIMEText(content,'plain','GBK') #设置字符编码
    msg.attach(body)

    msgto = ['test@lianlian.com'] # 收件人地址 多个联系人，格式是['aa@163.com'; 'bb@163.com']
    msgfrom = 'test@junbao.net' # 寄信人地址 ,
    msg['subject'] = 'IMD_EBM_'+CURRENTDAY  #主题
    msg['date']=time.ctime() #时间
    msg['Cc']='bb@junbao.net' #抄送人地址 多个地址不起作用


    mailuser = 'test'  # 用户名
    mailpwd = 'test' #密码

    try:
        smtp = smtplib.SMTP()
        smtp.connect(r'smtp.junbao.net')# smtp设置
        smtp.login(mailuser, mailpwd) #登录
        smtp.sendmail(msgfrom, msgto, msg.as_string()) #发送
        smtp.close()
    except Exception, e:
        print e
   

if __name__ == '__main__':
    logging.info('开始发送邮件.....')
    statinfo=os.stat(r"D:\report\IMD_EBM.xlsx")
    modifiedtime=time.localtime(statinfo.st_mtime)
    stmtime=time.strftime('%Y',modifiedtime) + time.strftime('%m', modifiedtime) +     time.strftime('%d', modifiedtime)

    if stmtime<> CURRENTDAY:
        print  "没有修改内容"
        time.sleep(2)
        exit(1)
    sendattachmail()
    logging.info('邮件发送成功。')
    logging.info('============================================================')