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
            self.netbw = config['netbw']
        if 'n_osds' in config:
            self.n_osds = config['n_osds']
        if 'placement' in config:
            self.placement = config['placement']
        if 'scheduler' in config:
            self.scheduler = config['scheduler']


class Simulator(event.EventSimulator):
    """Active Flash simulator
    """
    def __init__(self, conf, js):
        super(Simulator, self).__init__()
        self.afs = activefs.ActiveFS(self, Config(conf))
        self.afs.submit_job(js)

    def report(self):
        print('\n-----------------------------------------')
        print('job:', self.afs.job.name)
        print('scheduler:', self.afs.config.scheduler)
        print('\nTask statistics')
        for task in self.afs.job.tasks.values():
            task.report()

        print('\nOSD busy intervals')
        for i in range(len(self.afs.osds)):
            intervals = [ (t.stat.t_submit, t.stat.t_complete) \
                          for t in self.afs.job.tasks.values() if t.osd == i]
            #print('OSD', i, intervals)
            """
            print('OSD', i, ['(%.2f,%.2f)' % \
                    (x,y) for x,y in sorted(intervals, key=lambda x: x[0])])
            """
            print('OSD', i,
                  '[%s]' % ', '.join('(%.2f,%.2f)' % (x,y) for x,y in \
                   sorted(intervals, key=lambda x: x[0])))

        print('\nSSD RW statistics')
        for i in range(len(self.afs.osds)):
            total_read = self.afs.osds[i].get_total_read()
            total_write = self.afs.osds[i].get_total_write()
            extra_read = self.afs.osds[i].get_extra_read()
            extra_write = self.afs.osds[i].get_extra_write()
            print('OSD', i, '(Total RW, Extra RW):',
                    total_read, total_write, ',', extra_read, extra_write)


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

    print('\nsimulation finished at %.3f' % finish)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

