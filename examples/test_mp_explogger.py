import multiprocessing as mp
import threading as tr
import explogger

import multiprocessing as mp

explogger.setup_logging(with_console=True)

import logging
logging.basicConfig()


def f(name):
    print('hello', name)
    with explogger.ExperimentLog('./exp.db', ntp_sync=False) as expl:
        data = {'a':1, 'b':2}
        expl.log("data1", data=data)

if __name__ == '__main__':

    with explogger.ExperimentLog('./exp.db', ntp_sync=False) as expl:

        data = {'a':3, 'b':4}
        expl.log("data0", data=data)

        p = mp.Process(target=f, args=('bob',))
        p.start()
        p.join()

        data = {'a':3, 'b':4}
        expl.log("data0", data=data)
