import time
import sys
import logging
from multiprocessing import Process
import experimentlog
import zmq
from experimentlog import MetaProxy
import traceback

# Port used for ZMQ communication
ZMQ_PORT = 3149

def start_experiment(args, kwargs):
    """Launch the ExperimentLog as a 0MQ server.
    This expects tuples of (fn, *args, **kwargs) to come in on port ZMQ_PORT as
    Python objects (using recv_pyobj() / send_pyobj())

    It responds with a (success, return_value) tuple. Success is True if no
    exception was thrown, and False if one was thrown. In the case of an exception,
    the second argument (return_value) is a tuple (exception, traceback).
    """

    stopped = False
    # set up the server to handle incoming requests
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % ZMQ_PORT)

    # create the object
    e = experimentlog.ExperimentLog(*args, **kwargs)
    while not stopped:
        # loop, waiting for a request
        cmd, args, kwargs = socket.recv_pyobj()
        try:
            fn = getattr(e,cmd)
            if callable(fn):
                retval = fn(*args, **kwargs)
            else:
                retval = fn
            # send back the return value
            socket.send_pyobj((True, retval), protocol=-1)
        except:
            # exception, return the full exception info
            info = sys.exc_info()
            tb = "\n".join(traceback.format_exception(*info, limit=20))
            socket.send_pyobj((False, (info[1], tb)), protocol=-1)
        # update stopped flag
        stopped = not e.opened


class LogProxy(object):
    """Proxy for an ExperimentLog object.
    Redirects calls and property accesses to the real, remote logging object
    """
    def __init__(self):
        # connect to the server
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:%s" % ZMQ_PORT)
        # make metadata work the same way as in the ExperimentLog
        self.meta = MetaProxy(self)

    def __getattr__(self, attr):
        if attr=='meta':
            return

        # redirect properties
        if attr in ['bindings', 'session_path', 'session_id', 't', 'in_run', 'random_seed']:
            self.socket.send_pyobj((attr,(),()))
            success, value = self.socket.recv_pyobj()
            if success:
                return value
            else:
                # deal with exceptions in the remote process
                logging.error(value[1])
                raise value[0]
        else:
            # redirect calls to the remote object
            def proxy(*args, **kwargs):
                self.socket.send_pyobj((attr, args, kwargs), protocol=-1)
                success, value = self.socket.recv_pyobj()
                if success:
                    return value
                else:
                    # deal with exceptions in the remote process
                    logging.error(value[1])
                    raise value[0]

            return proxy


class ZMQLog(object):
    def __init__(self, *args, **kwargs):
        """Start the log server. Objects to access the server are produced by get_proxy()"""
        self.process = Process(target=start_experiment, args=(args, kwargs))
        self.process.start()

    def get_proxy(self):
        """Return a proxy object which behaves like ExperimentLog, but actually
        redirects to a remote process with a single ExperimnetLog object"""
        return LogProxy()



import random

def log_remote(name):
    logger = LogProxy()
    for i in range(20):
        time.sleep(random.random()*0.1)
        logger.log(name, data={"name":name, "id":i})

if __name__=="__main__":
    # basic test if the remote logging is working...
    m = ZMQLog("my_multi.db", ntp_sync=False)

    log = LogProxy()
    t = str(time.time())

    log.enter(t)
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
    log.leave()
    log.close()

    import sqlite3
    conn = sqlite3.connect("my_multi.db")
    results = conn.execute("SELECT * FROM log WHERE session=?", (session_id,)).fetchall()
    for r in results:
        print r
    conn.close()