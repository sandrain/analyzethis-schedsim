#!/usr/bin/env python

from itertools import *
from functools import reduce
import activefs
import event
import scipy
import numpy as np

""" File Server: simulate a single Gluster file server
"""
class Server(event.TimeoutEventHandler):
    # Class initialization function
    def __init__(self, ev, host_id, config):
        # Initialize basic data
        self.host_id = host_id
        self.config = config
        self.ev = ev
        # Setup the host's file system
        self.afs = activefs.ActiveFS(ev, config)

    def get_name(self):
        return 'Server-' + str(self.host_id)

    def get_state(self):
        s = ""
        for j in range(len(self.afs.osds)):
            s += "%s - %s\n" % (self.get_name(), self.afs.osds[j].get_state())
        return s

    # Save a file to a storage server
    def save_file(self, myfile, mount_point):
        print "Not implemented yet"

    # 
    def assign_to_mount_point(self, mount_point, storage_server):
        print "Not implemented yet"

    def store_file(self, file_to_store, storage_server):
        print "Not implemented yet"

    def copy_file_to_host(self, dest_host, myfile):
        print "Not implemented yet"

    def mv_file_to_host(self, dest_host, myfile):
        print "Not implemented yet"

    # Code duplicated from ActiveSimulator class
    def report(self):
        print '\n-----------------------------------------'
        print 'job: %s' % self.afs.job.name
        print 'scheduler: %s' % self.afs.config.scheduler
#        print 'device scheduler: %s' % self.afs.config.deviceScheduler
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

class Client():
    def __init__(self, host_id, config):
        # Setup the host
        self.host_id = host_id
        
        # Create a virtual mount point on the client

    def get_name(self):
        return 'Client-' + str(self.host_id)

    def get_state(self):
        return self.get_name()

    def submit_workflow(self, workflow):
        # If a task is already mapped, simply assign it to the correct host/tq
        print "Not implemented yet"

        # if a task is not mapped, call the scheduler
        print "Nothing to do yet"
