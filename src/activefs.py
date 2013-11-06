#!/usr/bin/env python

from itertools import *
from functools import reduce
import random
import job
import event
import sched

class ActiveFlash(event.TimeoutEventHandler):
    """Active flash element
    """
    def __init__(self, ev, id, afs):
        self.exit = False
        self.id = id
        self.ev = ev
        self.afs = afs
        self.tq = []
        self.ev.register_module(self)
        self.idle_event = event.TimeoutEvent('idle', 1, self)
        self.idle_event.set_disposable()
        self.task_event = event.TimeoutEvent('task', 0, self)
        self.running = None

    def submit_task(self, task):
        self.tq.append(task)
        task.submitted(self.ev.now())
        self.try_execute_task()

    def try_execute_task(self):
        if self.running != None:
            return

        if len(self.tq) > 0:
            task = self.tq.pop(0)
            self.running = task
            self.task_event.set_timeout(task.runtime)
            self.task_event.set_context(task)
            task.started(self.ev.now())
            self.ev.register_event(self.task_event)
            """
        else:
            pass
            if not self.exit:
                self.ev.register_event(self.idle_event)
                """

    def handle_timeout(self, e):
        print('ActiveFlash[', self.id,']: event=', e.name)

        if e.name == 'exit':
            self.exit = True
            return
        elif e.name == 'task':
            self.handle_timeout_task(e)
        else:   # idle falls here
            self.try_execute_task()

    """Handles task completion event
    """
    def handle_timeout_task(self, e):
        task = self.running
        if e.get_context() != task:
            raise SystemExit('BUG')

        task.completed(self.ev.now())
        self.afs.task_completed(task, self)
        self.running = None
        self.try_execute_task()


class ActiveFS(event.TimeoutEventHandler):
    """ActiveFS
    Currently, we assume that only a single job is submitted
    """
    def __init__(self, ev, config):
        self.exit = False
        self.ev = ev
        self.config = config
        self.osds = [ ActiveFlash(ev, n, self) \
                        for n in range(self.config.n_osds) ]
        self.ev.register_module(self)
        self.pq = []    # pre(pared) q, all data files are ready

        if self.config.scheduler == 'input':
            self.scheduler = sched.SchedInput(self)
        else:
            self.scheduler = sched.SchedRR(self)

    def submit_job(self, js):
        try:
            self.job = job.ActiveJob(js)
        except:
            raise

        self.populate_files()
        self.tq = list(self.job.tasks.values())
        self.scheduler.job_submitted()

    def populate_files(self):
        if self.config.placement == 'explicit':
            return

        if self.config.placement == 'random':
            self.populate_files_random()
        else:   # including 'rr', others fallback here
            sorted_files = list(map(lambda x: x[1],
                                sorted(self.job.files.items())))
            valid_files = list(filter(lambda x: x.size > 0, sorted_files))
            for (f, osd) in zip(valid_files,
                                cycle(range(self.config.n_osds))):
                f.set_location(osd)

    def task_completed(self, task, osd):
        self.advance()

    def check_termination(self):
        if len(self.tq) + len(self.pq) == 0:
            self.ev.terminate()
            return True
        else:
            return False

    def request_data_transfer(self, task):
        transfer_from = [ 0 for x in range(len(self.osds)) ]
        transfer_list = [ task.osd ]
        for f in task.input:
            if not f.is_replicated(task.osd):
                transfer_from[f.location] += f.size
                task.account_transfer(f)
                transfer_list += [ f ]

        transfer_total = reduce(lambda x, y: x+y, transfer_from)
        if transfer_total > 0:
            delay = max(transfer_from) / self.config.netbw
            e = event.TimeoutEvent('transfer', delay, self)
            e.set_context(transfer_list)
            self.ev.register_event(e)

    def handle_transfer_complete(self, e):
        li = e.get_context()
        osd = li[0]
        for f in li[1:]:
            f.add_replica(osd)

    def advance(self):
        if self.check_termination():
            return
        self.handle_prepared_tasks()
        self.handle_ready_tasks()

    def handle_prepared_tasks(self):
        prepared = [ task for task in self.tq if task.is_prepared() ]
        if len(prepared) > 0:
            for task in self.tq[:]:
                if task in prepared:
                    self.tq.remove(task)
            self.pq += prepared
            self.scheduler.task_prepared(prepared)

            for task in prepared:
                self.request_data_transfer(task)

    def handle_ready_tasks(self):
        ready = [ task for task in self.pq if task.is_ready() ]
        if len(ready) > 0:
            for task in self.pq[:]:
                if task in ready:
                    self.pq.remove(task)

            for task in ready:
                self.osds[task.osd].submit_task(task)

    def handle_timeout(self, e):
        print('ActiveFS: event=', e.name)
        if self.exit:
            return

        if e.name == 'exit':
            self.exit = True
            return
        elif e.name == 'transfer':
            self.handle_transfer_complete(e)
        else:
            pass

        self.advance()


