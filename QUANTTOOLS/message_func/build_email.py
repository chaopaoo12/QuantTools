import pandas as pd

def build_head():
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
    return(head)

def build_table(data, title, sub_tile = None):
    pd.set_option('display.max_colwidth', -1)
    df_html = data.to_html(escape=False)
    body = \
        """
        <body>

        <div align="center" class="header">
            <!--标题部分的信息-->
            <h1 align="center">{title}</h1>
            <h2 align="center">{sub_tile}</h2>
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
        """.format(title=title,sub_tile=sub_tile,df_html=data)
    return(body)


def build_email(head, body, *args):
    html_msg = "<html>" + head + body
    for i in args:
        html_msg += i
    html_msg = html_msg + "</html>"
    html_msg = html_msg.replace('\n','')
    return(html_msg)