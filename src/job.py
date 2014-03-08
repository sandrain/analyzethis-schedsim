#!/usr/bin/env python

from functools import reduce
from lxml import etree

class DataFile:
    """Data file representation
    Note that we use negative size for uncreated (not yet ready) files.
    """
    def __init__(self, name, obj):
        try:
            self.name = name
            self.size = obj['size']
            if 'location' in obj:
                self.location = obj['location']
            else:
                self.location = -1
        except:
            raise
        else:
            self.producer = None
            self.ready = False
            if self.size > 0:
                self.ready = True
            self.replica = []

    def set_location(self, osd):
        self.location = osd

    def add_replica(self, osd):
        self.replica += [ osd ]

    def set_producer(self, task):
        self.producer = task

    def is_replicated(self, osd):
        return self.location == osd or osd in self.replica

    def is_ready(self):
        return self.size > 0 and self.location >= 0

    def make_ready(self, osd):
        self.size = abs(self.size)
        self.location = osd


class ActiveTaskStat:
    """Task runtime statistics"""
    def __init__(self):
        self.t_submit = 0
        self.t_start = 0
        self.t_complete = 0
        #self.t_transfer = 0
        self.f_transfers = []      # list of transferred data files

    def submitted(self, now):
        self.t_submit = now

    def started(self, now):
        self.t_start = now

    def completed(self, now):
        self.t_complete = now

    def dump(self):
        print 'submit    = {0:.3f}'.format(self.t_submit)
        print 'start     = {0:.3f}'.format(self.t_start)
        print 'complete  = {0:.3f}'.format(self.t_complete)
        if len(self.f_transfers) == 0:
            print 'No data transfers'
            return

        print 'transferB = %d' % \
                reduce(lambda x, y: x+y, [tf.size for tf in self.f_transfers])
        print 'transfers = %d %s' %  (len(self.f_transfers),
             [(str(tf.name), tf.location, tf.size) for tf in self.f_transfers])


class ActiveTask:
    """Task description"""
    def __init__(self, name, obj, files):
        try:
            self.runtime = obj['runtime']
            self.name = name
            self.input = [ files[x] for x in obj['input'] ]
            self.output = [ files[x] for x in obj['output'] ]
            for f in self.output:
                f.set_producer(self)
        except:
            raise
        else:
            self.osd = -1
            self.stat = ActiveTaskStat()

    def set_osd(self, osd):
        self.osd = osd

    def depends(self):
        return [ f.producer for f in self.input ]

    def submitted(self, now):
        self.stat.submitted(now)

    def started(self, now):
        self.stat.started(now)

    def completed(self, now):
        self.stat.completed(now)
        for f in self.output:
            f.make_ready(self.osd)

    def account_transfer(self, f):
        self.stat.f_transfers += [ f ]

    def is_prepared(self):
        for f in self.input:
            if f.size < 0:
                return False
        return True

    def is_ready(self):
        if self.is_prepared() == False:
            return False
        for f in self.input:
            if not f.is_replicated(self.osd):
                return False
        return True

    def report(self):
        print '\ntask: %s' % self.name
        print 'osd      = %d' % self.osd
        self.stat.dump()


class ActiveJob:
    """Job description"""
    def __init__(self, job):
        try:
            self.name = job['name']
            self.files = dict(zip(job['files'].keys(),
                                  map(DataFile,
                                      job['files'].keys(),
                                      job['files'].values())))
            self.tasks = dict(zip(job['tasks'].keys(),
                                  map(ActiveTask,
                                      job['tasks'].keys(),
                                      job['tasks'].values(),
                                      [self.files]*len(job['tasks']))))
        except:
            raise

class Workflow:
    """Workflow description"""
    def __init__(self, root):
        try:
            ns = { 'ns': 'http://pegasus.isi.edu/schema/DAX' }
            jobs = root.findall('ns:job', namespaces=ns)
            self.name = jobs[0].attrib['namespace'] + '_' + str(len(jobs))

            for job in jobs:
                task = {}
                task['runtime'] = float(job.attrib['runtime'])

        except:
            raise

