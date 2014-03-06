#!/usr/bin/env python

import sys
import argparse
import textwrap
import json
import pdb

import event
import activefs

class PassiveSimulator(event.EventSimulator):
    """This class has been added to simulate the situations when the jobs are
    running in the host system.
    """
    pass

class ActiveSimulator(event.EventSimulator):
    """Active Flash simulator
    """
    def __init__(self, options):
        event.EventSimulator.__init__(self)
        self.afs = activefs.ActiveFS(self, options)

        with open(options.script) as f:
            js = json.load(f)
        self.afs.submit_job(js)

    def report(self):
        print '\n-----------------------------------------'
        print 'job: %s' % self.afs.job.name
        print 'scheduler %s:' % self.afs.config.scheduler
        print '\nTask statistics'
        for task in self.afs.job.tasks.values():
            task.report()

        print '\nOSD busy intervals'
        for i in range(len(self.afs.osds)):
            intervals = [ (t.stat.t_start, t.stat.t_complete)
                          for t in self.afs.job.tasks.values() if t.osd == i]
            print 'OSD %d [%s]' % \
                    (i, ', '.join('(%.2f, %.2f)' % (x,y) for x,y in
                             sorted(intervals, key=lambda x: x[0])))

        print '\nSSD RW statistics'
        print '%-3s%11s%11s%11s%11s' % \
                ('OSD', 'Total R', 'Total W', 'Extra R', 'Extra W')
        for i in range(len(self.afs.osds)):
            total_read = self.afs.osds[i].get_total_read()
            total_write = self.afs.osds[i].get_total_write()
            extra_read = self.afs.osds[i].get_extra_read()
            extra_write = self.afs.osds[i].get_extra_write()
            print repr(i).rjust(3),
            print repr(total_read).rjust(10), repr(total_write).rjust(10),
            print repr(extra_read).rjust(10), repr(extra_write).rjust(10)

        total_transfer = reduce(lambda x, y: x+y,
                           [ osd.get_extra_read() for osd in self.afs.osds ])

        print '\nTotal data transfer = %d bytes (%.3f MB)' % \
                    (total_transfer, total_transfer / (2**20))

"""main program
"""
def main():
    args_description = textwrap.dedent("""\
            ActiveFS scheduling simulator. Currently only simulates a single
            job execution. The default options are identical to:

                --netbw 104857600 --osds 4 --scheduler rr --placement rr

            The following job schedulers are available:
              rr: round-robin (default)
              input: input-locality, task is placed where input file is
              input-enhanced: input based, but also tries to parallelize

            The following data placement policies are available:
              rr: round-robin (default)
              random: input files are placed randomly
              explit: follows the 'location' field in json job script

            """)

    parser = argparse.ArgumentParser(
                formatter_class=argparse.RawDescriptionHelpFormatter,
                description=args_description)
    parser.add_argument('-b', '--netbw', type=int, default=104857600,
                        help='network bandwith (bytes/sec)')
    parser.add_argument('-n', '--osds', type=int, default=4,
                        help='number of osds')
    parser.add_argument('-s', '--scheduler', type=str, default='rr',
                        help='job scheduler')
    parser.add_argument('-p', '--placement', type=str, default='rr',
                        help='dataplacement policy')
    parser.add_argument('-d', '--debug', default=False,
                        help='enters pdb session for debugging',
                        action='store_true')
    parser.add_argument('-e', '--eventlog', default=False,
                        help='prints eventlogs', action='store_true')
    parser.add_argument('script', type=str, help='job script in JSON')
    args = parser.parse_args()

    if args.debug:
        print("debug is enabled, launch pdb...")
        pdb.set_trace()     # comment out this to disable pdb

    sim = ActiveSimulator(args)
    sim.prepare()
    finish = sim.run()
    sim.report()

    print('\nsimulation finished at %.3f' % finish)
    return 0


if __name__ == '__main__':
    sys.exit(main())

