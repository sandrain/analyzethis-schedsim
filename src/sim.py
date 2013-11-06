#!/usr/bin/env python

import sys
import argparse
import textwrap
import json
import pdb

import event
import activefs

class Simulator(event.EventSimulator):
    """Active Flash simulator
    """
    def __init__(self, options):
        super(Simulator, self).__init__()
        self.afs = activefs.ActiveFS(self, options)

        with open(options.script) as f:
            js = json.load(f)
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

"""main program
"""
def main():
    args_description = textwrap.dedent("""\
            ActiveFS scheduling simulator. Currently only simulates a single
            job execution. The default options are identical to:
                --netbw 1048576 --osds 4 --scheduler rr --placement rr

            """)

    parser = argparse.ArgumentParser(
                formatter_class=argparse.RawDescriptionHelpFormatter,
                description=args_description)
    parser.add_argument('-b', '--netbw', type=int, default=104857600,
                help='set the network bandwith (bytes/sec)')
    parser.add_argument('-n', '--osds', type=int, default=4,
                        help='set the number of osds')
    parser.add_argument('-s', '--scheduler', type=str, default='rr',
                        help='set the scheduler, rr or input')
    parser.add_argument('-p', '--placement', type=str, default='rr',
                    help='set dataplacement policy (rr, explicit, or random)')
    parser.add_argument('-d', '--debug', default=False,
                        help='launch pdb in the main',
                        action='store_true')
    parser.add_argument('script', type=str, help='job script in JSON')
    args = parser.parse_args()

    if args.debug:
        print("debug is enabled, launch pdb...")
        pdb.set_trace()     # comment out this to disable pdb

    sim = Simulator(args)
    sim.prepare()
    finish = sim.run()
    sim.report()

    print('\nsimulation finished at %.3f' % finish)
    return 0


if __name__ == '__main__':
    sys.exit(main())

