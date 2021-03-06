import time
import sys
import logging
from multiprocessing import Process
import zmq
import traceback

import collections

import logging
import daiquiri
daiquiri.setup(level=logging.INFO)

# logging.basicConfig()

import explogger
from explogger import zmq_log


import random

def log_remote(name):
    logger = zmq_log.LogProxy()
    for i in range(20):
        time.sleep(random.random()*0.1)
        logger.log(name, data={"name":name, "id":i})

if __name__=="__main__":
    # basic test if the remote logging is working...
    m = zmq_log.ZMQLog("my_multi.db", ntp_sync=False)

    log = m.get_proxy()
    t = str(time.time())
    log.enter(t)

    pub = zmq_log.LogProxyPub()
    for i in range(100):
        pub.log('test', data={'x':1})
        time.sleep(1./20)

    print(pub.test)


    session_id = log.session_id
    print("Multiple asynchronous writes...")
    p1 = Process(target=log_remote, args=("Alpha",))
    p2 = Process(target=log_remote, args=("Bravo",))
    p3 = Process(target=log_remote, args=("Charlie",))

    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()
    print("Completed.")


    print(log.meta_dataframe())

    log.leave()
    log.close()

    # import sqlite3
    # conn = sqlite3.connect("my_multi.db")
    # results = conn.execute("SELECT * FROM log WHERE session=?", (session_id,)).fetchall()
    # for r in results:
    #     print(r)
    # conn.close()