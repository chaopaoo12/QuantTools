#coding=utf-8
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart  # 构建邮件头信息，包括发件人，接收人，标题等
from email.header import Header

def send_email(mail_title, msg, date):
    smtpserver = "smtp.qq.com"
    smtpport = 465
    msg_from ='738105084@qq.com'                                 #发送方邮箱
    passwd ='hqsjjrhdtvnvbeab'                                   #填入发送方邮箱的授权码
    msg_to =['738105084@qq.com']                                 #收件人邮箱
    subject = mail_title                                              #主题
    if isinstance(msg, str):
        html = msg + ' wrong in {date}'.format(date= date)
    else:
        html = msg
    content_html = MIMEText(html, "html", "utf-8")
    msg = MIMEMultipart('related')
    msg['Subject'] = Header(mail_title)
    msg["From"] = msg_from
    msg.attach(content_html)
    try:
        s = smtplib.SMTP_SSL("smtp.qq.com",465)#邮件服务器及端口号
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print("发送成功")
    except(s.SMTPException) as e:
        print("发送失败")
    finally:
        s.quit()