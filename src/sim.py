#!/usr/bin/env python

import sys
import json
import sched
import event
import activefs

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


class Simulator(event.EventSimulator):
    """Active Flash simulator
    """
    def __init__(self, config, js):
        super(Simulator, self).__init__()
        afs = activefs.ActiveFS(self, config)
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

