#-*-coding:utf-8-*-
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart


def send_email_to(to, error_str):
    sender_mail = '80617252@qq.com'
    sender_pass = 'rnuvglzoffigcafh'

    # 设置总的邮件体对象，对象类型为mixed
    msg_root = MIMEMultipart('mixed')
    msg_root['From'] = '80617252@qq.com<80617252@qq.com>'
    msg_root['To'] = to
    subject = 'some erro in your project need you handle it'
    msg_root['subject'] = Header(subject, 'utf-8')

    text_info = error_str
    text_sub = MIMEText(text_info, 'plain', 'utf-8')
    msg_root.attach(text_sub)

    try:
        sftp_obj =smtplib.SMTP('smtp.qq.com', 25)
        print 'haha'
        sftp_obj.login(sender_mail, sender_pass)
        sftp_obj.sendmail(sender_mail, to, msg_root.as_string())
        sftp_obj.quit()
        print 'sendemail successful!'

    except Exception as e:
        print 'sendemail failed next is the reason'
        print e


if __name__ == '__main__':
    # 可以是一个列表，支持多个邮件地址同时发送，测试改成自己的邮箱地址
    to = '80617252@qq.com'
    error_str = 'hello world'
    send_email_to(to, error_str)
                                                       
