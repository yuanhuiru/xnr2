#-*- coding: utf-8 -*-
# sgkuzxxajgshehcj
import string
import smtplib
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


reload(sys)
sys.setdefaultencoding('utf-8')

# msginfo:邮件内容
# Subject:邮件主题
def sendqqmail(msginfo, Subject='', html=False):
	_user = "1615899476@qq.com"
	_pwd = "sgkuzxxajgshehcj"  # 填写第一步获取的密码，非QQ密码哦
	_tostr = "bingqulee@outlook.com,1615899476@qq.com"
	_to = string.splitfields(_tostr, ",")
	msg = MIMEMultipart('alternative')
	msg["Subject"] = Subject
	msg["From"] = _user
	msg["To"] = _tostr
	if html:
		text = MIMEText(msginfo, 'html', 'utf-8')
		msg.attach(text)
	else:
		text = MIMEText(msginfo.encode("utf-8"))
		msg.attach(text)
	try:
		s = smtplib.SMTP_SSL("smtp.qq.com", 465)
		s.login(_user, _pwd)
		s.sendmail(_user, _to, msg.as_string())
		s.quit()
		print "Success!"
	except smtplib.SMTPException, e:
		print "Falied,%s" % e

if __name__ == '__main__':

	sendqqmail('哈哈哈哈哈哈哈')

