#!/usr/bin/env python

import sys
import argparse
import textwrap
import json
import pdb
import scipy
import numpy as np
from lxml import etree
from os.path import expanduser

import event
import activefs
import cluster
import job

# The scheduling library is always required since it is for instance used
# to determine file placement. See the README file for details about how
# to setup and use that library.
import py_lat_module

class PassiveSimulator(event.EventSimulator):
    """This class has been added to simulate the situations when the jobs are
    running in the host system.
    """
    pass

class DistributedPlatformSimulator():
    def __init__(self, options):
        self.options = options

        # We initialize the scheduling module
        print "Using the platform configuration file %s" % self.options.file
        rc = py_lat_module.lat_module_init ("verbose",
                                            "1",
                                            "ini_config_file",
                                            self.options.file)
        if (rc != 0):
            print "ERROR: lat_module_init() failed (ret: %d)\n" % rc
        print "Success.\n"

        print "Initializing the AFE scheduler..."
        rc = py_lat_module.lat_device_sched_init ();
        if (rc != 0):
            print "ERROR: lat_device_sched_init() failed (ret: %d)\n" % rc
        print "Success.\n";

        print "Initializing the host scheduler..."
        rc = py_lat_module.lat_host_sched_init ();
        if (rc != 0):
            print "ERROR: lat_device_sched_init() failed (ret: %d)\n" % rc
        print "Success.\n";

        print "Initializing the meta scheduler..."
        rc = py_lat_module.lat_meta_sched_init ();
        if (rc != 0):
            print "ERROR: lat_meta_sched_init() failed (ret: %d)\n" % rc
        print "Success.\n"

        self.py_lat_module = py_lat_module

        # Setup the virtual platform
        self.cluster = cluster.Cluster(options)

    def check_termination(self):
        return self.afs.check_termination()

    def report(self):
        self.cluster.report()

    def prepare(self):
        # Try to perform a static scheduling of the workflow, using the input
        # file, which contains the actual workflow
        # Loading the scheduling library
        (rc, static_placement) = py_lat_module.lat_meta_sched_workflow (self.options.script)
        if (rc != 0):
            raise

        # Prepare the workflow (basically parse the file representing the
        # workflow
        self.workflow = job.Workflow (static_placement, self.options)

        self.cluster.prepare_workflow(self.workflow)

        # "Prepare" the event system
        self.cluster.prepare()

        # Submit the workflow
        self.cluster.submit_workflow(self.workflow)

    def run(self):
        return self.cluster.run()

class ActiveSimulator(event.EventSimulator):
    """Active Flash simulator
    """
    def __init__(self, options):
        event.EventSimulator.__init__(self)
        self.options = options
        options.host_type = 'server'

        # We initialize the scheduling module
        print "Using the platform configuration file %s" % self.options.file
        rc = py_lat_module.lat_module_init ("verbose",
                                            "1",
                                            "ini_config_file",
                                            self.options.file)
        if (rc != 0):
            print "ERROR: lat_module_init() failed (ret: %d)\n" % rc
        print "Success.\n"

        print "Initializing the AFE scheduler..."
        rc = py_lat_module.lat_device_sched_init ();
        if (rc != 0):
            print "ERROR: lat_device_sched_init() failed (ret: %d)\n" % rc
        print "Success.\n";

        print "Initializing the host scheduler..."
        rc = py_lat_module.lat_host_sched_init ();
        if (rc != 0):
            print "ERROR: lat_device_sched_init() failed (ret: %d)\n" % rc
        print "Success.\n";

        options.py_lat_module = py_lat_module

        # Initialize the simulation from a file system point of view
        self.afs = activefs.ActiveFS (self, options)

        # Prepare the workflow (basically parse the file representing the
        # workflow)
        self.workflow = job.Workflow (options.script, options)
        self.afs.prepare_workflow (self.workflow)

        # Submit the workflow.
        # Note this phase will parse the workflow file.
        # Also note that in the context of a fully distributed system, a static
        # scheduling of the workflow may be done at submition
        # time.
        self.afs.submit_workflow (self.workflow)

        # the json is now replaced by xml, the format of pegasus workflow
        # generator
#        with open(options.script) as f:
#            js = json.load(f)
#        self.afs.submit_job(js)

    def check_termination(self):
        return self.afs.check_termination()

    def report(self):
        print '\n-----------------------------------------'
        print 'job: %s' % self.afs.job.name
        print 'scheduler: %s' % self.afs.config.scheduler
        print 'device scheduler: %s' % self.afs.config.deviceScheduler
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

def parse_config_file(conffile):
    print "Parsing configuration file (%s)" % conffile
    


"""main program
"""
def main():
    args_description = textwrap.dedent("""\
            ActiveFS scheduling simulator. Currently only simulates a single
            job execution. The default options are identical to:

                --netbw 262144000 --osds 4 --scheduler rr --placement rr --core 2 --deviceScheduler firstAvailable --file my_file.conf

            netbw is 250 MB/s by default.

            The following job schedulers are available:
              rr: round-robin (default)
              locality: input-locality, task is placed where input file is
              minwait: task is placed where waiting time is minimal
              hostonly: only host is used
              hostreduce: reduce tasks are scheduled to hybrid
              core: number of cores per AFE (supposed to be homogeneous across
                    the platform at the moment)
	      file: configuration file describing the experiment

            The following device schedulers are available:
                firstAvailable: the first available device's core (i.e., core
                                that does not execute any task) is used

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
    parser.add_argument('-N', '--nodes', type=int, default=0,
                        help='number of server nodes (set to 0 to simulate a single host)')
    parser.add_argument('-n', '--osds', type=int, default=4,
                        help='number of AFEs per node')
    parser.add_argument('-x', '--hostspeed', type=float, default=2.0,
                        help='host clock speed (e.g. x2, x4, ...)')
    parser.add_argument('-s', '--scheduler', type=str, default='rr',
                        help='job scheduler')
    parser.add_argument('-S', '--deviceScheduler', type=str,
                        default='firstAvailable',
                        help='device scheduler')
    parser.add_argument('-p', '--placement', type=str, default='rr',
                        help='dataplacement policy')
    parser.add_argument('-d', '--debug', default=False,
                        help='enters pdb session for debugging',
                        action='store_true')
    parser.add_argument('-e', '--eventlog', default=False,
                        help='prints eventlogs', action='store_true')
    parser.add_argument('-c', '--cores', type=int, default=1,
                        help='number of cores per AFE')
    parser.add_argument('-f', '--file', type=str, default='',
                        help='configuration file')

    parser.add_argument('script', type=str, help='job script in XML')
    args = parser.parse_args()

    if args.debug:
        print("debug is enabled, launch pdb...")
        pdb.set_trace()     # comment out this to disable pdb

    # Because it is quite difficult to eschange complex data structures from
    # python to C and C to python, we rely on a configuration file to describe
    # the experiment and all the configuration parameters.
    if (args.file == ''):
        # If we do not have a config file, we will create a temporary one
        # based on the command line parameters.
        tmp_file = expanduser('~') + "/.afs-schedsim/tmp_config_file.cfg"
        print "Creating temporary config file %s" % tmp_file
        tmpfile = open (tmp_file, 'w')
        tmpfile.write ("[AFE]\n")
        tmpfile.write ("cores_per_afe = %d\n" % args.cores) 
        tmpfile.write ("\n[SERVERS]\n")
        tmpfile.write ("number_afes = %d\n" % args.osds)
        tmpfile.write ("number_hosts = %d\n" % args.nodes)
        tmpfile.close ()
        args.file = tmp_file
    parse_config_file (args.file)

    if (args.nodes == 0):
        sim = ActiveSimulator(args)
    else:
        sim = DistributedPlatformSimulator(args)

    sim.prepare()
    finish = sim.run()
    sim.report()

    print('\nsimulation finished at %.3f' % finish)
    return 0


if __name__ == '__main__':
    sys.exit(main())

