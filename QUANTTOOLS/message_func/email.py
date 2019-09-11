#coding=utf-8
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart  # 构建邮件头信息，包括发件人，接收人，标题等
from email.header import Header
import pandas as pd

def get_html(data, title, yesterday):
    pd.set_option('display.max_colwidth', -1)
    df_html = data.to_html(escape=False)
    head = \
        """
        <head>
            <meta charset="utf-8">
            <STYLE TYPE="text/css" MEDIA=screen>

                table.dataframe {
                    border-collapse: collapse;
                    border: 2px solid #a19da2;
                    /*居中显示整个表格*/
                    margin: auto;
                }

                table.dataframe thead {
                    border: 2px solid #91c6e1;
                    background: #f1f1f1;
                    padding: 10px 10px 10px 10px;
                    color: #333333;
                }

                table.dataframe tbody {
                    border: 2px solid #91c6e1;
                    padding: 10px 10px 10px 10px;
                }

                table.dataframe tr {

                }

                table.dataframe th {
                    vertical-align: top;
                    font-size: 14px;
                    padding: 10px 10px 10px 10px;
                    color: #105de3;
                    font-family: arial;
                    text-align: center;
                }

                table.dataframe td {
                    text-align: center;
                    padding: 10px 10px 10px 10px;
                }

                body {
                    font-family: 宋体;
                }

                h1 {
                    color: #5db446
                }

                div.header h2 {
                    color: #0002e3;
                    font-family: 黑体;
                }

                div.content h2 {
                    text-align: center;
                    font-size: 28px;
                    text-shadow: 2px 2px 1px #de4040;
                    color: #fff;
                    font-weight: bold;
                    background-color: #008eb7;
                    line-height: 1.5;
                    margin: 20px 0;
                    box-shadow: 10px 10px 5px #888888;
                    border-radius: 5px;
                }

                h3 {
                    font-size: 22px;
                    background-color: rgba(0, 2, 227, 0.71);
                    text-shadow: 2px 2px 1px #de4040;
                    color: rgba(239, 241, 234, 0.99);
                    line-height: 1.5;
                }

                h4 {
                    color: #e10092;
                    font-family: 楷体;
                    font-size: 20px;
                    text-align: center;
                }

                td img {
                    /*width: 60px;*/
                    max-width: 300px;
                    max-height: 300px;
                }

            </STYLE>
        </head>
        """
    body = \
        """
        <body>

        <div align="center" class="header">
            <!--标题部分的信息-->
            <h1 align="center">{title}</h1>
            <h2 align="center">{yesterday}</h2>
        </div>

        <hr>

        <div class="content">
            <!--正文内容-->
            <h2> </h2>

            <div>
                <h4></h4>
                {df_html}

            </div>
            <hr>

            <p style="text-align: center">

            </p>
        </div>
        </body>
        """.format(title=title,yesterday=yesterday,df_html=df_html)
    html_msg = "<html>" + head + body + "</html>"
    html_msg = html_msg.replace('\n','')
    return(html_msg)


def send_email(data, date,  mail, title):
    smtpserver = "smtp.qq.com"
    smtpport = 465
    msg_from ='738105084@qq.com'                                 #发送方邮箱
    passwd ='hqsjjrhdtvnvbeab'                                   #填入发送方邮箱的授权码
    msg_to =['738105084@qq.com']                                 #收件人邮箱
    subject = title                                              #主题
    if isinstance(data, str):
        html = data + ' wrong in {date}'.format(date= date)
    else:
        html = get_html(data, title, date)
    content_html = MIMEText(html, "html", "utf-8")
    msg = MIMEMultipart('related')
    msg['Subject'] = Header(mail)
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