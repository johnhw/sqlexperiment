import time
import sys
from multiprocessing import Process, Lock, Queue, Pipe, RLock
import experimentlog
import zmq
from experimentlog import MetaProxy

# Port used for ZMQ communication
ZMQ_PORT = 3149

def start_experiment(args, kwargs):
    stopped = False    
    # set up the server to handle incoming requests
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % ZMQ_PORT)
    
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
        except Exception, exc:            
            # exception, return the full exception info
            info = sys.exc_info()
            socket.send_pyobj((False, info), protocol=-1)
        # update stopped flag
        stopped = not e.in_run        


class LogProxy(object):
    """Proxy for an ExperimentLog object. 
    Redirects calls and property accesses to the real, remote logging object
    """
    def __init__(self):
        # connect to the server
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:%s" % ZMQ_PORT)
        self.meta = MetaProxy(self)    
    
    def __getattr__(self, attr):             
        if attr=='meta':
            return
            
        # redirect properties
        if attr in ['bindings', 'session_path', 'session_d', 't', 'in_run', 'random_seed']:
            self.socket.send_pyobj((attr,(),()))
            success, value = self.socket.recv_pyobj()                
            if success:                
                return value
            else:
                # deal with exceptions in the remote process
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
                    raise value[0]
            return proxy
            

class ZMQLog(object):
    def __init__(self, *args, **kwargs):
        self.process = Process(target=start_experiment, args=(args, kwargs))
        self.process.start()
        
    def get_proxy(self):
        return LogProxy()
    
        
if __name__=="__main__":
    m = ZMQLog("my_multi.db", ntp_sync=False)
    proxy = m.get_proxy()
    while 1:
        proxy.log("mouse", data={"x":5, "y":6})
        time.sleep(1)        
        print proxy.meta.stage