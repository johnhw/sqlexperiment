import time
from multiprocessing import Process, Lock, Queue, Pipe, RLock
import experimentlog


def start_experiment(args, kwargs, in_q, out_q):
    stopped = False
    e = experimentlog.ExperimentLog(*args, **kwargs)
    while not stopped:        
        cmd, args, kwargs = in_q.get()               
        try:
            fn = getattr(e,cmd)
            retval = fn(*args, **kwargs)            
            out_q.put(retva)
        except Exception, exc:            
            out_q.put(exc)
        # update stopped flag
        stopped = not e.in_run        


class LogProxy(object):
    def __init__(self, in_q, out_q):
        self.in_q = in_q
        self.out_q = out_q
        
    def __getattr__(self, attr):            
            def proxy(*args, **kwargs):                
                self.out_q.put((attr, args, kwargs))
                print self.in_q.get()
            return proxy
        

class MPLog(object):
    def __init__(self, *args, **kwargs):
        self.in_q = Queue()
        self.out_q = Queue()
        self.process = Process(target=start_experiment, args=(args, kwargs, self.in_q, self.out_q))
        self.process.start()
        
    def get_proxy(self):
        return LogProxy(self.out_q, self.in_q)
    
        
if __name__=="__main__":
    m = MPLog("my_multi.db", ntp_sync=False)
    proxy = m.get_proxy()
    while 1:
        proxy.log("mouse", data={"x":5, "y":6})
        time.sleep(1)
