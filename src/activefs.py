#!/usr/bin/env python

import event

class ActiveFlash(event.TimeoutEventHandler):
    """Active flash element
    """
    def __init__(self, ev, id):
        self.id = id
        self.ev = ev
        self.tq = []
        self.ev.register_module(self)

    def submit_task(self, task):
        pass

    def handle_timeout(self, event):
        print('ActiveFlash[', self.id,']: event=', event.name)


class ActiveFS(event.TimeoutEventHandler):
    """ActiveFS
    """
    def __init__(self, ev, config):
        self.ev = ev
        self.config = config
        self.osds = []
        for n in range(self.config['n_osds']):
            self.osds += [ ActiveFlash(ev, n) ]
        self.ev.register_module(self)

    def submit_job(self, js):
        try:
            self.job = sched.ActiveJob(js)
        except:
            raise

        self.populate_files()

    def handle_timeout(self, event):
        print('ActiveFS: event=', event.name)

    def populate_files(self):
        pass

