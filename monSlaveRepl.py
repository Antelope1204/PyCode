#!/usr/bin/python
# -*- coding: utf-8 -*-
#filename：monSlaveRepl.py
#author：Antelop.X

import os
import MySQLdb
import sys
import os.path
import urllib
import urllib2
import logging

#mysql configure
MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "cdcsync",
    "passwd": "123456"
}
#log configure
logging.basicConfig(
    filename='/data/tmp/mysql_repliaction_monitor.log',
    filemode='a',
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(funcName)s %(levelname)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S',
    level=logging.INFO
)
def monitor_MySQL_replication():
    #monitor mysql slave status
    status = True
    try:
        conn = MySQLdb.connect(**MYSQL_SETTINGS)
        cur = con.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("SHOW SLAVE STATUS;")
        res = cur.fetchone()
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

def send_mail(receiver,content,title):
    # send alarm to somebody
    rtx_url = 'http://www.example.com:11770/sendRtxByPost'
    data = {
        "appId"     : 6,
        "appKey"    : 'password',
        "username"  : receiver,
        "title"     : title,
        "content"   : content
    }
    postdata = urllib.urlencode(data)
    req = urllib2.Request(rtx_url,postdata)
    return urllib2.urlopen(req)

def clear_log():
    #clear monitor log
    log_file='/data/tmp/mysql_repliaction_monitor.log'
    size = os.path.getsize(log_file)/(1024*1024)
    if size >= 100:
        os.remove(log_file)
        logging.info('%s has been remove' %(log_file))

if __name__ == "__main__":
    clear_log()
    mail_receiver = "happy;"
    if monitor_MySQL_replication():
        send_mail(receiver=mail_receiver,content='slave replication error',title=get_hostname())