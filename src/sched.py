#!/usr/bin/env python

class DataFile:
    """Data file representation
    Note that we use negative size for uncreated (not yet ready) files.
    """
    def __init__(self, obj):
        try:
            self.size = obj['size']
            if 'location' in obj:
                self.location = obj['location']
            else:
                self.location = []
        except:
            raise
        else:
            self.producer = None
            self.ready = False
            if self.size > 0:
                self.ready = True

    def set_replica(self, osd):
        self.location += [ osd ]

    def set_producer(self, task):
        self.producer = task

    def is_ready(self):
        return self.size > 0

    def make_ready(self):
        self.size = abs(self.size)


class ActiveTaskStat:
    """Task runtime statistics"""
    def __init__(self):
        self.t_submit = 0
        self.t_start = 0
        self.t_complete = 0
        self.t_transfer = 0
        self.f_transfers = []      # list of transferred data files

    def submitted(self, now):
        self.t_submit = now

    def started(self, now):
        self.t_started = now

    def completed(self, now):
        self.t_completed = now


class ActiveTask:
    """Task description"""
    def __init__(self, obj, files):
        try:
            self.runtime = obj['runtime']
            in_files = obj['input']
            out_files = obj['output']
            self.input = [ files[x] for x in in_files ]
            self.output = [ files[x] for x in out_files ]
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
            f.make_ready()


class ActiveJob:
    """Job description"""
    def __init__(self, job):
        try:
            self.name = job['name']
            self.files = dict(zip(job['files'].keys(),
                                  map(DataFile, job['files'].values())))
            self.tasks = dict(zip(job['tasks'].keys(),
                                  map(ActiveTask, job['tasks'].values(),
                                      [self.files]*len(job['tasks']))))
        except:
            raise

