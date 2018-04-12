import time
import sys
import logging
from multiprocessing import Process
import zmq
import traceback

import collections

from .core import ExperimentLog, MetaProxy

from .extract import meta_dataframe

# Port used for ZMQ communication
ZMQ_PORT = 3149




class ZMQLog(object):
    def __init__(self, *args, **kwargs):
        """Start the log server. Objects to access the server are produced by get_proxy()"""
        self.process = Process(target=start_experiment, args=(args, kwargs))
        self.process.start()

    def get_proxy(self):
        """Return a proxy object which behaves like ExperimentLog, but actually
        redirects to a remote process with a single ExperimnetLog object"""
        return LogProxy()


def start_experiment(args, kwargs):
    """Launch the ExperimentLog as a 0MQ server.
    This expects tuples of (fn, *args, **kwargs) to come in on port ZMQ_PORT as
    Python objects (using recv_pyobj() / send_pyobj())

    It responds with a (success, return_value) tuple. Success is True if no
    exception was thrown, and False if one was thrown. In the case of an exception,
    the second argument (return_value) is a tuple (exception, traceback).
    """

    stopped = False
    # set up the server to handle incoming requests with polling
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % ZMQ_PORT)
    poll = zmq.Poller()
    poll.register(socket, zmq.POLLIN)

    # create the object
    e = ExperimentLog(*args, **kwargs)
    while not stopped:

        # poll
        socks = dict(poll.poll(500))  # 500 ms for timeout
        if socks.get(socket) == zmq.POLLIN:

            # loop, waiting for a request
            cmd, args, kwargs = socket.recv_pyobj()
            try:

                if cmd == "meta_dataframe":
                    retval = meta_dataframe(e.cursor)
                else:
                    fn = getattr(e,cmd)
                    if isinstance(fn, collections.Callable):
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


