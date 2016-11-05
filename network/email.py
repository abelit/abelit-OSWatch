'''
Created on Sep 23, 2016

@author: abelit
'''

import cx_Oracle
import datetime
import smtplib
import tempfile
from email.message import Message
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart

today = datetime.datetime.now()

msg = MIMEMultipart()
msg['From'] = 'Reports Service <reports@company.intranet>'
msg['To'] = 'receipients@company.intranet'
msg['Subject'] = 'Monthly employee report %d/%d ' % (today.month, today.year)

db = cx_Oracle.connect('hr', 'hrpwd', 'localhost/xe')
cursor = db.cursor()
cursor.execute("select * from employees order by 1")
report = tempfile.NamedTemporaryFile()
report.write("<table>")
for row in cursor:
  report.write("<tr>")
  for field in row:
    report.write("<td>%s</td>" % field)
  report.write("</tr>")
report.write("</table>")
report.flush()
cursor.close()
db.close()

attachment = MIMEBase('application', 'vnd.ms-excel')
report.file.seek(0)
attachment.set_payload(report.file.read())
encode_base64(attachment)
attachment.add_header('Content-Disposition', 'attachment;filename=emp_report_%d_%d.xls' % (today.month, today.year))
msg.attach(attachment)

emailserver = smtplib.SMTP("localhost")
emailserver.sendmail(msg['From'], msg['To'], msg.as_string())
emailserver.quit()