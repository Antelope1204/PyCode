#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import sys
import copy
import time
import datetime
import random
import threading

import MySQLdb
import Queue

g_mysql_addr = 'IP1:PORT1 | IP2:PORT2'
g_mysql_user = 'puser'
g_mysql_password = '123'
g_mysql_db = ''
g_mysql_charset = 'utf8'

g_intervative_timeout = 1800
g_trx_wait_timeout = 0
g_queue_size = 8192
g_speed_rate = 1
g_new_connection_only = False 
g_allow_lost_sql = False

###### Do not Modify the variables below ######
g_threads_pool = {}
g_thread_lock = threading.Lock()

MYSQL_LOG_TYPE = 1
DBPROXY_LOG_TYPE = 2

"""
The general log
"""
class GeneralLog:

    def __init__(self, log_type):
        self.log_type = log_type
        self.query_type = ""
        self.session_id = 0
        self.start_time = 0
        self.sql = ""

    def add_sql(self, sql):
        self.sql += "\n"
        self.sql += sql

    def parse(self, line):
        if self.log_type == MYSQL_LOG_TYPE:
            self.parse_mysql_log(line)
        elif self.log_type == DBPROXY_LOG_TYPE:
            self.parse_dbproxy_log(line)

    def parse_mysql_log(self, line):
        if len(line) < 6:
            self.sql = line
            return 0

        if line[0] == '/' and "Version" in line and "started with" in line:
            return 1
        if line[0] == 'T' and "Tcp port" in line and "Unix socket" in line:
            return 1
        if line[0] == 'T' and "Time" in line and "Id" in line and "Command" in line and "Argument" in line:
            return 1

        i = 0
        log_time = None

        if line[0] >= '0' and line[0] <= '9' and len(line) > 15:
            try:
                log_time = time.strptime(line[:15], "%y%m%d %H:%M:%S")
                i += 15
            except Exception, e:
                pass

        if log_time != None:
            self.start_time = int(time.mktime(log_time))

        length = len(line)
        while i < length and (line[i] == ' ' or line[i] == '\t'):
            i += 1

        # Get Query ID
        neg_queryid = 0
        if i + 1 < length and line[i] == '-' and line[i+1] >= '1' and line[i+1] <= '9':
            neg_queryid = 1
            i += 1

        j = i + get_int_end(line[i:])
        if i == j:
            self.sql = line
            return 0

        self.session_id = long(line[i:j])
        if neg_queryid != 0:
            self.session_id = 0 - self.session_id

        i = j
        while i < length and (line[i] == ' ' or line[i] == '\t'):
            i += 1

        # Get Query Type
        if i < length and 'B' <= line[i] and line[i] <= 'T':
            j = i + 1
            while j < length and line[j] >= 'a' and line[j] <= 'z':
                j += 1
            if j >= length or (line[j] != ' ' and line[j] != '\t'):
                self.session_id = 0
                self.query_type = "Query"
                self.sql = line
                return 0
            if line[i:].startswith("Init DB"):
                self.query_type = "Init DB"
                i += 8
                while i < length and (line[i] == ' ' or line[i] == '\t' or line[i] == '\n'):
                    i += 1
                if "Access denied" not in line[i:] and i < length:
                    self.sql = "USE `" + line[i:] + "`" 
            elif line[i:j] == "Query" or line[i:j] == "Execute":
                self.query_type = "Query"
                i += 5
                while i < length and (line[i] == ' ' or line[i] == '\t'):
                    i += 1
                self.sql = line[i:]
            elif line[i:j] == "Connect":
                self.query_type = "Connect"
                self.sql = ""
                if "Access denied" not in line[i:] and " on " in line[i:]:
                    j = length - 1
                    if line[j] != ' ' and line[j] != '\t':
                        while j > i and (line[j-1] != ' ' and line[j-1] != '\t'):
                            j -= 1
                        if j < length:
                            self.sql = "USE `" + line[j:] + "`"
            elif line[i:].startswith("Quit"):
                self.query_type = "Quit"
                self.sql = "Quit"
            elif line[i:].startswith("Binlog Dump"):
                self.query_type = "Binlog"
                self.sql = ""
            elif line[i:j] == "Statistics" or \
                 line[i:j] == "Ping" or \
                 line[i:j] == "Kill" or \
                 line[i:j] == "Field" or \
                 line[i:j] == "Prepare" or \
                 line[i:j] == "Close":
                self.query_type = line[i:j]
                self.sql = ""
            elif line[i:j] == "Or" or \
                 line[i:j] == "Group" or \
                 line[i:j] == "Order" or \
                 line[i:j] == "Limit":
                self.session_id = 0
                self.query_type = "Query"
                self.sql = line
            else:
                self.session_id = 0
                print "unrecognized general log[%s]:%s" % (line[i:j], line[i:])
                return 1
        else:
            self.session_id = 0
            self.query_type = "Query"
            self.sql = line
            return 0

    def parse_dbproxy_log(self, line):

        length = len(line)
        if length < 60 or line[0] != '[':
            self.session_id = 0
            self.query_type = "Query"
            self.sql = line
            return

        log_time = None

        # parse query time
        if line[1] >= '0' and line[1] <= '9' and len(line) > 15:
            try:
                log_time = time.strptime(line[1:20], "%Y-%m-%d %H:%M:%S")
            except Exception, e:
                print e
                pass

        if log_time != None:
            self.start_time = int(time.mktime(log_time))
        else:
            self.session_id = 0
            self.query_type = "Query"
            self.sql = line
            return

        # parse query type
        i = line.find('[', 10)
        j = -1
        if i > 0:
            j = line.find(']', i)

        query_type = 'Query'
        if i > 0 and j > i :
            query_type = line[i+1:j]
            if query_type == 'QUIT':
                self.query_type = 'Quit'
                self.sql = 'Quit'
            if query_type == 'CONN':
                self.query_type = 'Connect'
            elif query_type == 'QURY':
                self.query_type = 'Query'

        # parse session id 
        if line[j+1] == '[':
            self.session_id = parse_session_id(line[j+2:])

        if self.session_id == 0:
            self.sql = line
            return

        if self.query_type == 'Quit':
            return

        # parse sql
        i = line.find(' ', 40)
        if i < 0 or i == length - 1:
            self.session_id = 0
            self.sql = line
            return

        if self.query_type == 'Query':
            self.sql = line[i+1:]
        else :
            end = i
            end = line[0:end].rfind('/') - 1
            if end > 0:
                end = line[0:end].rfind('/') - 1
                if end > 0:
                    i = line[0:end].rfind('/') + 1
                    if i > 0 and i <= end:
                        self.sql = 'USE `' + line[i:end+1] + "`"
        
"""
Working Thread
"""
class WorkerThread(threading.Thread):

    def __init__(self, session_id, queue):
        threading.Thread.__init__(self) # this method must be called
        self._session_id = session_id
        self._queue = queue # main thread will put sql in this queue, worker thread will fetch and execute
        self._conn = None
        self._cursor = None

    def open(self):

        idx = self._session_id % len(g_mysql_addr)
        host = g_mysql_addr[idx]["host"]
        port = g_mysql_addr[idx]["port"]

        # print "Sesson %d choosed addr[%d] -> %s:%d" % (self._session_id, idx, host, port)
        self._conn = MySQLdb.connect(host = host, \
                                     port = port, \
                                     user = g_mysql_user, \
                                     passwd = g_mysql_password, \
                                     db = g_mysql_db, \
                                     charset = g_mysql_charset)
        self._conn.autocommit(True)
        self._cursor = self._conn.cursor()

    def close(self):
        if self._conn != None:
            self._conn.close()
        if self._cursor != None:
            self._cursor.close()

    def get_queue(self):
        return self._queue

    def run(self):

        global g_threads_pool
        global g_intervative_timeout

        try:
            self.open()
            sql = self._queue.get(block = True, timeout = g_intervative_timeout)
            while len(sql) > 0:
                # print "%d|%d|%s" % (self._session_id, self.ident, sql)
                if sql == "Quit":
                    break
                try:
                    self._cursor.execute(sql)
                except Exception, e:
                    if len(sql) > 0:
                        print "Execute %d SQL: %s failed." % (self._session_id, sql), e

                    if e[0] == 2006 or e[0] == 2013 or e[0] == 2003:
                        break;

                sql = self._queue.get(block = True, timeout = g_intervative_timeout)
        except Exception, e:
            print e
        self.close()
        g_thread_lock.acquire()
        g_threads_pool.pop(self._session_id)
        g_thread_lock.release()

def get_int_end(line):
    i = 0
    while i < len(line) and line[i] >= '0' and line[i] <= '9':
        i += 1
    return i

def current_milliseconds():
    c_time = datetime.datetime.now();
    return int(time.mktime(c_time.timetuple())) * 1000L + c_time.microsecond / 1000

def parse_session_id(line):
    parts = line.split(':')
    if len(parts) < 2:
        return 0
    ip = sum([256L**j*int(i) for j,i in enumerate(parts[0].split('.')[::-1])])
    return (ip << 16) + long(parts[1])
    

"""
Every session has a thread, dispatch_log() will dispatch a general_log to a working thread
"""
def dispatch_log(general_log):

    global g_threads_pool
    global g_trx_wait_timeout

    session_id = general_log.session_id
    if session_id == 0:
        return

    worker = None
    # If there is no matching working thread in g_threads_pool, we create it
    g_thread_lock.acquire()
    if session_id in g_threads_pool:
        worker = g_threads_pool[session_id]
    elif not g_new_connection_only or general_log.query_type == "Connect" or general_log.query_type == "Init DB":
        queue = Queue.Queue(g_queue_size)
        worker = WorkerThread(session_id, queue)
        g_threads_pool[session_id] = worker
        worker.setDaemon(True)
        worker.start()
    g_thread_lock.release()

    if worker == None:
        return

    queue = worker.get_queue()
    if len(general_log.sql) > 0:
        if g_trx_wait_timeout > 0:
            try:
                # if time out, may be there is a deadlock, this replayer will hang, so we try to rollback transactions
                queue.put(general_log.sql, block = True, timeout = g_trx_wait_timeout)
            except Queue.Full:
                g_thread_lock.acquire()
                for t_sid, t_worker in g_threads_pool.items():
                    if t_sid != session_id and not t_worker.get_queue().full():
                        print "Session [%d] Rollback Transaction" % (t_sid,)
                        t_worker.get_queue().put("ROLLBACK /*Replayer Rollback Transaction*/", block = False)
                g_thread_lock.release()
        elif not g_allow_lost_sql or not queue.full() or general_log.sql == "Quit":
            queue.put(general_log.sql)
    elif general_log.query_type == "Quit":
        queue.put("")

def write_current_line(f, no):
    f.seek(0)
    f.write("%d\n" % (no,))

def parse_mysql_addr(mysql_addr):
    global g_mysql_addr
    addr_list = g_mysql_addr.split('|') 
    g_mysql_addr = []
    for s in addr_list:
        s = s.strip()
        sp_addr = s.split(':')
        if len(sp_addr) != 2:
            print "Parse MySQL Addr [%s] failed." %s (s)
            return False
        addr = {}
        addr["host"] = sp_addr[0].strip()
        addr["port"] = int(sp_addr[1].strip())
        g_mysql_addr.append(addr)
    return True

def main():

    if len(sys.argv) < 2:
        print "Usage: %s <filename>" % (sys.argv[0],)
        return 1

    log_file_name = sys.argv[1]
    print "log_file_name = %s" % (log_file_name,)

    if not parse_mysql_addr(g_mysql_addr):
        print "Init MySQL addr failed"
        return 1

    pos_file = open("./processed_line", "w")
    # Control the send log speed not exceed the online Qps
    last_log_time = 0;
    last_process_time = current_milliseconds()


    log_type = 0
    count = 0
    # Read and process mysql general log
    general_log = GeneralLog(0)
    for line in open(log_file_name):

        count += 1
        if len(line) == 0:
            continue

        if line[len(line) - 1] == '\n':
            line = line[:len(line)-1]

        if len(line) == 0:
            continue

        write_current_line(pos_file, count)
        if count == 1:
            if line[0] == '[' and \
                    len(line) > 28 and \
                    line[1] >= '0' and \
                    line[1] <= '9' and \
                    line[20] == '.' and \
                    line[27] == ']' and \
                    line[28] == '[':
                log_type = DBPROXY_LOG_TYPE
                print "This is DBProxy log"
            else:
                log_type = MYSQL_LOG_TYPE
                print "This is MySQL General log"

        tmp_log = GeneralLog(log_type)
        tmp_log.parse(line)

        if len(general_log.sql) > 64 * 1024 * 1024:
            print "SQL is too long:", general_log.sql
            return

        if g_speed_rate > 0 and last_log_time != 0 \
                and tmp_log.start_time != 0 and last_log_time != tmp_log.start_time:
            sleep_time = (tmp_log.start_time - last_log_time) * 1000  - (current_milliseconds() - last_process_time)
            sleep_time  /= g_speed_rate

            if sleep_time > 1000:
                sleep_time = 1000
            if sleep_time > 0:
                time.sleep(0.001 * sleep_time);

        if tmp_log.start_time > last_log_time:
            last_log_time = tmp_log.start_time;

        if general_log.session_id == 0:
            general_log = copy.deepcopy(tmp_log)
        if len(tmp_log.sql) > 0:
            if tmp_log.session_id == 0:
                general_log.add_sql(tmp_log.sql)
            else:
                dispatch_log(general_log)
                general_log = copy.deepcopy(tmp_log)
                last_process_time = current_milliseconds()
        elif len(general_log.sql) > 0:
            dispatch_log(general_log)
            general_log = copy.deepcopy(tmp_log)
            last_process_time = current_milliseconds()

    if len(general_log.sql) > 0:
        dispatch_log(general_log)
        general_log = copy.deepcopy(tmp_log)
    
    pos_file.close()
    g_thread_lock.acquire()
    for t_sid, t_worker in g_threads_pool.items():
        t_worker.get_queue().put("Quit")
    g_thread_lock.release()

    thread_count = 1
    while thread_count > 0:
        g_thread_lock.acquire()
        thread_count = len(g_threads_pool)
        g_thread_lock.release()
        time.sleep(1)


# There is a python bug: ImportError: Failed to import _strptime because the import lockis held by
# another thread
_throwaway = time.strptime('20110101','%Y%m%d')

if __name__ == '__main__':
    main()
