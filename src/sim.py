#!/usr/bin/env python

import sys
import json
import sched
import event

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


class ActiveFlash(event.TimeoutEventHandler):
    """Active flash element
    """
    def __init__(self, ev, id):
        self.id = id
        self.ev = ev
        self.tq = []
        self.ev.register_module(self)

    def submit_task(self, task):
        pass

    def handle_timeout(self, event):
        print('ActiveFlash[', self.id,']: event=', event.name)


class ActiveFS(event.TimeoutEventHandler):
    """ActiveFS
    """
    def __init__(self, ev, config):
        self.ev = ev
        self.config = config
        self.osds = []
        for n in range(self.config['n_osds']):
            self.osds += [ ActiveFlash(ev, n) ]
        self.ev.register_module(self)

    def submit_job(self, js):
        try:
            self.job = sched.ActiveJob(js)
        except:
            raise

        self.populate_files()

    def handle_timeout(self, event):
        print('ActiveFS: event=', event.name)

    def populate_files(self):
        pass


class Simulator(event.EventSimulator):
    """Active Flash simulator
    """
    def __init__(self, config, js):
        super(Simulator, self).__init__()
        afs = ActiveFS(self, config)
        afs.populate_files()

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
    except:
        raise

    sim = Simulator(config, js)
    sim.prepare()
    finish = sim.run()
    sim.report()

    print("simulation finished at ", finish)

    sys.exit(0)

