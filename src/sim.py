#!/usr/bin/env python

import pdb
import sys
import json
import event
import activefs

class Config:
    """simulation configurations
    """
    def __init__(self, config):
        self.netbw = 104857600.0       # 100 MB/s
        self.n_osds = 4                # 4 osds
        self.placement = 'rr'
        self.scheduler = 'rr'

        if 'netbw' in config:
            netbw = config['netbw'] * (2**20)
        if 'n_osds' in config:
            n_osds = config['n_osds']
        if 'placement' in config:
            placement = config['placement']
        if 'scheduler' in config:
            scheduler = config['schedule']


class Simulator(event.EventSimulator):
    """Active Flash simulator
    """
    def __init__(self, conf, js):
        super(Simulator, self).__init__()
        self.afs = activefs.ActiveFS(self, Config(conf))
        self.afs.submit_job(js)

    def report(self):
        for task in self.afs.job.tasks.values():
            task.report()


def main(argv=None):
    if argv == None or len(argv) != 2:
        print('Usage: sim.py <conf file> <job file>')
        return 1

    try:
        with open(argv[0]) as f:
            conf = json.load(f)
        with open(argv[1]) as f:
            js = json.load(f)
    except:
        raise

    pdb.set_trace()

    sim = Simulator(conf, js)
    sim.prepare()
    finish = sim.run()
    sim.report()

    print("\nsimulation finished at ", finish)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
