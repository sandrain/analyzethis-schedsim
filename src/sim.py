#!/usr/bin/env python

import sys
import argparse
import textwrap
import json
import pdb
import scipy
import numpy as np
from lxml import etree

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

        tree = None
        with open(options.script) as f:
            try:
                tree = etree.parse(f)
            except:
                raise

        root = tree.getroot()

        self.afs.submit_workflow(root)

        # the json is now replaced by xml, the format of pegasus workflow
        # generator
#        with open(options.script) as f:
#            js = json.load(f)
#        self.afs.submit_job(js)

    def report(self):
        print '\n-----------------------------------------'
        print 'job: %s' % self.afs.job.name
        print 'scheduler %s:' % self.afs.config.scheduler
        print 'core(s) per AFE: %d' % self.afs.config.cores
        print '\nTask statistics'
        for task in self.afs.job.tasks.values():
            task.report()

        """OSD statistics
        """
        print '\nOSD busy intervals'
        busy = []
        for i in range(len(self.afs.osds)):
            intervals = [ (t.stat.t_start, t.stat.t_complete)
                          for t in self.afs.job.tasks.values() if t.osd == i]
            if len(intervals) > 0:
                busy += [ reduce(lambda x, y: x+y,
                                 map(lambda (x, y): y-x, intervals)) ]
            else:
                busy += [ 0.0 ]
            print 'OSD %d: %.3f sec\n\t[%s]' % \
                    (i, busy[-1],
                     ', '.join('(%.2f, %.2f)' % (x,y) for x,y in
                             sorted(intervals, key=lambda x: x[0])))

        util_mean = scipy.mean(busy) / self.afs.ev.current * 100
        util_std = scipy.std(busy) / self.afs.ev.current * 100

        print '\nOSD mean utilization = %.3f' % util_mean
        print 'OSD std utilization = %.3f' % util_std

        """SSD statistics
        """
        print '\nSSD RW statistics'
        print '%-3s%11s%11s%11s%11s' % \
                ('OSD', 'Total R', 'Total W', 'Extra R', 'Extra W')
        total_read = 0
        total_write = 0
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
        reads = map(lambda x: x.get_total_read(), self.afs.osds)
        writes = map(lambda x: x.get_total_write(), self.afs.osds)

        rmean = scipy.mean(reads)
        wmean = scipy.mean(writes)
        wsum = np.sum(writes)
        rsum = np.sum(reads)
        rstd = scipy.std(reads)
        wstd = scipy.std(writes)

        print '\nTotal data transfer = %d bytes (%.3f MB)' % \
                    (total_transfer, float(total_transfer) / (2**20))
        print 'SSD mean read = %d bytes (%.3f MB)' % \
                    (rmean, float(rmean) / (2**20))
        print 'SSD mean write = %d bytes (%.3f MB)' % \
                    (wmean, float(wmean) / (2**20))
        print 'SSD std read = %d bytes (%.3f MB)' % \
                    (rstd, float(rstd) / (2**20))
        print 'SSD std write = %d bytes (%.3f MB)' % \
                    (wstd, float(wstd) / (2**20))
        print 'SSD total writes = %d bytes (%.3f MB)' % \
                    (wsum, float(wsum) / (2**20))
        print 'SSD total reads = %d bytes (%.3f MB)' % \
                    (rsum, float(rsum) / (2**20))
        print 'SSD write coefficient of variation = %.3f' % \
                    (wstd / wmean)

"""main program
"""
def main():
    args_description = textwrap.dedent("""\
            ActiveFS scheduling simulator. Currently only simulates a single
            job execution. The default options are identical to:

                --netbw 262144000 --osds 4 --scheduler rr --placement rr --core 2

            netbw is 250 MB/s by default.

            The following job schedulers are available:
              rr: round-robin (default)
              locality: input-locality, task is placed where input file is
              minwait: task is placed where waiting time is minimal
              hostonly: only host is used
              hostreduce: reduce tasks are scheduled to hybrid
              core: number of cores per AFE (supposed to be homogeneous across the
                    platform at the moment)

            The following data placement policies are available:
              rr: round-robin (default)
              random: input files are placed randomly
              explit: follows the 'location' field in json job script

            """)

    parser = argparse.ArgumentParser(
                formatter_class=argparse.RawDescriptionHelpFormatter,
                description=args_description)
    parser.add_argument('-b', '--netbw', type=int, default=262144000,
                        help='network bandwith (bytes/sec)')
    parser.add_argument('-r', '--runtime', type=float, default=1.0,
                        help='runtime slowdown factor (default 1.0)')
    parser.add_argument('-n', '--osds', type=int, default=4,
                        help='number of osds')
    parser.add_argument('-x', '--hostspeed', type=float, default=2.0,
                        help='host clock speed (e.g. x2, x4, ...)')
    parser.add_argument('-s', '--scheduler', type=str, default='rr',
                        help='job scheduler')
    parser.add_argument('-p', '--placement', type=str, default='rr',
                        help='dataplacement policy')
    parser.add_argument('-d', '--debug', default=False,
                        help='enters pdb session for debugging',
                        action='store_true')
    parser.add_argument('-e', '--eventlog', default=False,
                        help='prints eventlogs', action='store_true')
    parser.add_argument('-c', '--cores', type=int, default=1,
                        help='number of cores per AFE')
    parser.add_argument('script', type=str, help='job script in XML')
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

