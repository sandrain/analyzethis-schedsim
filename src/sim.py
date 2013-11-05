#!/usr/bin/env python

import sys
import heapq
from itertools import count
from json import load
import sched

class TimeoutEvent:
    def __init__(self, timeout, handler):
        self.timeout = timeout
        self.handler = handler

    def timeout(self):
        self.handler.timeout(self)


class TimeoutEventHandler:
    def timeout(self, event):
        pass


class Events:
    """Main event queue for the simulation
    """
    def __init__(self):
        self.pq = []
        self.counter = itertools.count()

    def register(self, event):
        heapq.heappush(self.pq,
                    [event.timeout, next(self.counter), event])


class Config:
    """simulation configurations
    """
    def __init__(self, config):
        self.netbw = 104857600       # 100 MB/s
        self.n_osds = 4              # 4 osds
        if 'netbw' in config:
            netbw = config['netbw'] * (2**20)
        if 'n_osds' in config:
            n_osds = config['n_osds']


class ActiveFlash(TimeoutEventHandler):
    """Active flash element
    """
    def __init__(self, id):
        self.id = id
        self.tq = []

    def submit_task(self, task):
        pass

    def timeout(self, event):
        pass


class ActiveFS(TimeoutEventHandler):
    """ActiveFS
    """
    def __init__(self, ev, config):
        self.ev = ev
        self.config = config
        for n in range(self.config_n_osds):
            self.osds += [ ActiveFlash(n) ]

    def submit_job(self, js):
        try:
            self.job = sched.ActiveJob(js)
        except:
            raise

        self.populate_files()

    def timeout(self, event):
        pass

    def populate_files(self):
        pass


class Simulator:
    """Main simulation framework
    """
    def __init__(self, config):
        try:
            afs = ActiveFs(Events(), Config(config))
        except:
            raise

    def start(self, js):
        try:
            afs.submit_job(js)
        except:
            raise

    def report(self):
        pass


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: ', sys.argv[0], ' <conf file> <job file>')
        sys.exit(1)

    try:
        with open(sys.argv[1]) as f:
            config = json.load(f)
        with open(sys.argv[2]) as f:
            js = json.load(f)

        sim = Simulator(config)
        sim.start(js)
    except:
        raise
    else:
        sim.report()
        sys.exit(0)

