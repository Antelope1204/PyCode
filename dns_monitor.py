#!/usr/bin/python
# -*- coding: utf-8 -*-
#filename: dns_monitor.py
#author: Antelop.X

import os
import subprocess
import datetime
import socket

DNS_ADMIN_STATUS = 'YES'
DNS_BIND_STATUS = 'YES'

def checkDNS():
    try:
        resDNS = subprocess.Popen("ps -ef | grep dns_admin",stout=subprocess.PIPE,shell=True)
        procDNS = resDNS.stout.readline()
        counts = len(procDNS)
        if counts < 4:
            DNS_ADMIN_STATUS = 'NONE'
            dt = datetime.datetime.now()
            fp = open('/home/mysql_dba/dnsadmin/dns_monitor.log',a)
            fp.write('DNS admin stop at %s\n' %(dt.strftime('%Y-%m-%d %H:%M:%S')))
            fp.close()
            subprocess.Popen('cd /home/mysql_dba/dnsadmin && nohup dns_admin.py &',shell=True)
    except exception, e:
            print e

def checkBIND():
    try:
        resBIND = subprocess.Popen("ps -ef | grep bind",stout=subprocess.PIPE,shell=True)
        procBIND = resBIND.stout.readline()
        counts = len(procBIND)
        if counts < 4:
            DNS_BIND_STATUS = 'NONE'
            dt = datetime.datetime.now()
            fp.open('/usr/local/named/bind_monitor.log',a)
            fp.write('DNS BIND stop at %s\n' %(dt.strftime('%Y-%m-%d %H:%M:%s')))
            fp.close()
            subprocess.Popen('/usr/local/named/sbin/named -u bind -c /usr/local/named/etc/named.conf',shell=True)
    except exception, e:
        print e

#send message interface @chengyi.wu
def send_message(telphone,message):
    url = "http://XXX"%(telphone,message)
    res = requests.get(url,{}).text
    if res == 'True':
        print('发送短信成功')
    else:
        print('发送短信失败')

def get_hostname():
    #get hostname of Linux
    return socket.gethostname()

def get_ip(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))[20:24])

if __name__ == "__main__":
    if DNS_ADMIN_STATUS == 'NONE' or DNS_BIND_STATUS == 'NONE':
        ip = get_ip('etho0')
        host = get_hostname()
        send_message('18510340022','DNS ALARM: \n DNS_ADMIN_STATUS: %s\n DNS_BIND_STATUS: \n IP: \n HOSTNAME: \n'%s(DNS_ADMIN_STATUS,DNS_BIND_STATUS_STATUS,ip,host))

