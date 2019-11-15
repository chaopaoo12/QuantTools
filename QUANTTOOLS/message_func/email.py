#coding=utf-8
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart  # 构建邮件头信息，包括发件人，接收人，标题等
from email.header import Header
from QUANTTOOLS.message_func.setting import smtpserver,smtpport,msg_from,passwd,msg_to

def send_email(mail_title, msg, date, smtpserver=smtpserver, smtpport=smtpport,msg_from=msg_from,msg_to=msg_to,passwd=passwd):
    smtpserver = smtpserver
    smtpport = smtpport
    msg_from = msg_from                            #发送方邮箱
    passwd =passwd                                 #填入发送方邮箱的授权码
    msg_to =msg_to                                 #收件人邮箱
    subject = mail_title                           #主题
    if isinstance(msg, str):
        html = msg + ' wrong in {date}'.format(date= date)
    else:
        html = msg
    content_html = MIMEText(html, "html", "utf-8")
    msg = MIMEMultipart('related')
    msg['Subject'] = Header(subject)
    msg["From"] = msg_from
    msg.attach(content_html)
    try:
        s = smtplib.SMTP_SSL(smtpserver,smtpport)#邮件服务器及端口号
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print("发送成功")
    except(s.SMTPException) as e:
        print("发送失败")
    finally:
        s.quit()