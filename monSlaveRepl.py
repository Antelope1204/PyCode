#!/usr/bin/python
# -*- coding: utf-8 -*-
#filename：monSlaveRepl.py
#author：Antelop.X

import os
import MySQLdb
import requests
import sys
import os.path
import urllib
import urllib2
import logging
import smtplib
from email.mime.text import MIMEText

#mysql configure
MYSQL_SETTINGS = {
    "host": "xx",
    "port": xx,
    "user": "dba",
    "passwd": "xx"
}
#log configure
logging.basicConfig(
    filename = '/data/tmp/mysql_repliaction_monitor.log',
    filemode = 'a',
    format = '%(asctime)s %(filename)s[line:%(lineno)d] %(funcName)s %(levelname)s %(message)s',
    datefmt = '%a, %d %b %Y %H:%M:%S',
    level = logging.INFO
)
#smtp configure

MAIL_REVIEW_SMTP_SERVER = 'xxx'
MAIL_REVIEW_SMTP_PORT = 25
MAIL_REVIEW_FROM_ADDR = 'DBA'
MAIL_REVIEW_FROM_PASSWORD = ''
MAIL_REVIEW_DBA_ADDR = ['xx@xx',]

#slave status default value
SLAVE_IO_RUNNING = None
SLAVE_SQL_RUNNING = None


#send message interface @chengyi.wu
def send_message(telphone,message):
    url = "xx"%(telphone,message)
    res = requests.get(url,{}).text
    if res == 'True':
        print('发送短信成功')
    else:
        print('发送短信失败')

#send mail alarm
def send_mail(to,sub,content):
    me = MAIL_REVIEW_FROM_ADDR+"<"+MAIL_REVIEW_FROM_ADDR+"@"+MAIL_REVIEEW_SMTP_SERVER+">"
    msg = MIMEText(content)
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = to
    msg['Cc'] = ';'.join(MAIL_REVIEW_DBA_ADDR)
    try:
        s = smtplib.SMTP()
        s.connect(MAIL_REVIEW_SMTP_SERVER)
        s.sendmail(me, to, msg.as_string())
        s.close()
        return True
    except Exception as e:
        print(e)
        return False

def monitor_MySQL_replication():
    #monitor mysql slave status
    status = True
    try:
        conn = MySQLdb.connect(**MYSQL_SETTINGS)
        cur = con.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("SHOW SLAVE STATUS;")
        res = cur.fetchone()
        SLAVE_IO_RUNNING=req['Slave_IO_Running']
        SLAVE_SQL_RUNNING=req['Slave_SQL_Running']
        if res['Slave_IO_Running'] == 'Yes' and res['Slave_SQL_Running'] == 'Yes':
            logging.info("OK!")
        else:
            logging.err("Wrong!")
            status = False
        cur.close()
        conn.close()
    except Exception as e:
            logging.err(e)
            status = False
    return status

def get_hostname():
    #get hostname of Linux
    return os.environ['HOSTNAME']

def clear_log():
    #clear monitor log
    log_file = '/data/tmp/mysql_repliaction_monitor.log'
    size = os.path.getsize(log_file)/(1024*1024)
    if size >= 100:
        os.remove(log_file)
        logging.info('%s has been remove' %(log_file))

if __name__ == "__main__":
    clear_log()
    if monitor_MySQL_replication():
        send_message("telephone", "Replication Error: Port_%s,Host_%s"%(MYSQL_SETTINGS['port'],MYSQL_SETTINGS['host']))
        send_mail([email, ], "MySQL Replication Alarm", "Slave_SQL_Running：%s Slave_IO_Running: %s "%(SLAVE_SQL_RUNNING,SLAVE_IO_RUNNING))
