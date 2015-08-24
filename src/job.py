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
            self.host = False
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

        """No need to care about the file location
        """
        if self.host == True:
            return True

        for f in self.input:
            if not f.is_replicated(self.osd):
                return False
        return True

    def report(self):
        print '\ntask: %s' % self.name
        if self.host == True:
            print 'osd      = Host'
        else:
            print 'osd      = %d' % self.osd
        self.stat.dump()

class Workflow:
    """ Class that represents a workflow and all possible ways to manipulate a
        workflow
    """
    def __init__(self, input_file, config):
        tree = None
        with open(input_file) as f:
            try:
                tree = etree.parse(f)
            except:
                raise

        self.root = tree.getroot()
        self.config = config

    def xmlToJson (self):
        """Nasty conversion from xml to json
        """
        ns = { 'ns':'http://pegasus.isi.edu/schema/DAX' }
        js = {}
        js['files'] = {}
        js['tasks'] = {}

        jobs = self.root.findall('ns:job', namespaces=ns)
        js['name'] = jobs[0].attrib['namespace'] + '_' + str(len(jobs))

        for job in jobs:
            task = {}
            task['runtime'] = float(job.attrib['runtime']) \
                                    * self.config.runtime
            task['input'] = []
            task['output'] = []
            for uses in job.findall('ns:uses', namespaces=ns):
                if uses.attrib['link'] == 'input':
                    task['input'] += [ uses.attrib['file'] ]
                else:
                    task['output'] += [ uses.attrib['file'] ]
                cfile = {}
                cfile['size'] = int(uses.attrib['size'])
                js['files'][uses.attrib['file']] = cfile
            js['tasks'][job.attrib['id'] + '-' + job.attrib['name']] = task

        for job in jobs:
            for uses in job.findall('ns:uses', namespaces=ns):
                if uses.attrib['link'] == 'output':
                    js['files'][uses.attrib['file']]['size'] = \
                            -1 * int(uses.attrib['size'])

        return js

    def get_root (self):
        return self.root

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

