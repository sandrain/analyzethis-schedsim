#!/usr/bin/env python

from itertools import *
from functools import reduce
from lxml import etree
import random
import job
import event
import sched

class ActiveFlash(event.TimeoutEventHandler):
    """Active flash element
    """
    def __init__(self, ev, id, afs):
        self.id = id
        self.ev = ev
        self.afs = afs
        self.tq = []
        self.ev.register_module(self)
        self.idle_event = event.TimeoutEvent('idle', 1, self)
        self.idle_event.set_disposable()
        self.task_event = event.TimeoutEvent('task', 0, self)
        self.running = None
        """statistics"""
        self.n_read = 0         # how much is read/written?
        self.n_written = 0
        self.n_extra_read = 0   # how much rw for data transfer?
        self.n_extra_written = 0

    def get_name(self):
        return 'ActiveFlash-' + str(self.id)

    def submit_task(self, task):
        self.tq.append(task)
        task.submitted(self.ev.now())
        self.try_execute_task()

    def get_qtime(self):
        wait = 0.0
        for task in self.tq:
            wait += task.runtime
        return wait

    def try_execute_task(self):
        if self.running != None:
            return

        if len(self.tq) > 0:
            task = self.tq.pop(0)
            self.running = task
            self.task_event.set_timeout(task.runtime)
            self.task_event.set_context(task)
            desc = '%s (%.3f sec) execution' % (task.name, task.runtime)
            self.task_event.set_description(desc)
            task.started(self.ev.now())
            self.ev.register_event(self.task_event)

    def handle_timeout(self, e):
        if self.afs.config.eventlog:
            print '(%.3f, %.3f) --- %s [%s] %s' \
                  % (e.registered, e.timeout, self.get_name(),
                     e.name, e.description)

        if e.name == 'task':
            self.handle_timeout_task(e)
        self.try_execute_task()


    """Handles task completion event
    """
    def handle_timeout_task(self, e):
        task = self.running
        if e.get_context() != task:
            raise SystemExit('BUG')

        task.completed(self.ev.now())
        self.afs.task_completed(task, self)
        self.update_data_rw(task)
        self.running = None
        self.try_execute_task()

    def update_data_rw(self, task):
        r, w = 0, 0
        if len(task.input) > 0:
            r = reduce(lambda x, y: x+y, [ f.size for f in task.input ])
        if len(task.output) > 0:
            w = reduce(lambda x, y: x+y, [ f.size for f in task.output ])
        self.data_read(r)
        self.data_write(w)

    def data_read(self, count):
        self.n_read += count

    def data_write(self, count):
        self.n_written += count

    def data_transfer_read(self, count):
        self.n_extra_read += count

    def data_transfer_write(self, count):
        self.n_extra_written += count

    def get_total_read(self):
        return self.n_read + self.n_extra_read

    def get_total_write(self):
        return self.n_written + self.n_extra_written

    def get_extra_read(self):
        return self.n_extra_read

    def get_extra_write(self):
        return self.n_extra_written


class ActiveHost(ActiveFlash):
    """Host
    """
    def __init__(self, ev, id, afs):
        self.id = id
        self.ev = ev
        self.afs = afs
        self.tq = []
        self.ev.register_module(self)
        self.idle_event = event.TimeoutEvent('idle', 1, self)
        self.idle_event.set_disposable()
        self.task_event = event.TimeoutEvent('task', 0, self)
        self.running = None
        """statistics"""
        self.n_read = 0         # how much is read/written?
        self.n_written = 0
        self.n_task = 0;        # how many tasks were processed here?
        self.last_written_osd = 0;  # write output in a RR order.

    def get_name(self):
        return 'Host-' + str(self.id)

    """here we assume that the internal NAND bandwidth is x2.56 faster than the
    external bandwidth. (iSSD, ICS'11)
    """
    def adjust_runtime(self, task):
        ssd_ch_bw = 40 * (1 << 20)
        ssd_n_ch = 32
        bw_ssd = ssd_ch_bw * ssd_n_ch

        r, w = 0, 0
        if len(task.input) > 0:
            r = reduce(lambda x, y: abs(x)+abs(y), [ f.size for f in task.input ])
        if len(task.output) > 0:
            w = reduce(lambda x, y: abs(x)+abs(y), [ f.size for f in task.output ])

        total_io = r + w
        t_ssd_io = float(total_io) / bw_ssd
        t_ssd_comp = task.runtime - t_ssd_io

        t_comp = t_ssd_comp / self.afs.config.hostspeed
        t_io = total_io / self.afs.config.netbw

        return t_comp + t_io

    def try_execute_task(self):
        if self.running != None:
            return

        if len(self.tq) > 0:
            task = self.tq.pop(0)
            self.running = task
            self.task_event.set_timeout(self.adjust_runtime(task))
            self.task_event.set_context(task)
            desc = '%s (%.3f sec) execution' % (task.name, task.runtime)
            self.task_event.set_description(desc)
            task.started(self.ev.now())
            self.ev.register_event(self.task_event)

    def update_data_rw(self, task):
        self.n_task += 1        # update the number of tasks processed

        r, w = 0, 0             # update data read/written
        if len(task.input) > 0:
            r = reduce(lambda x, y: x+y, [ f.size for f in task.input ])
        if len(task.output) > 0:
            w = reduce(lambda x, y: x+y, [ f.size for f in task.output ])
        self.data_read(r)
        self.data_write(w)

        for f in task.input:
            self.afs.osds[f.location].data_transfer_read(f.size)
        for f in task.output:
            if self.last_written_osd < self.afs.config.osds:
                f.location = self.last_written_osd
                self.last_written_osd += 1
            else:
                f.location = self.last_written_osd = 0
            self.afs.osds[f.location].data_transfer_write(f.size)


class ActiveFS(event.TimeoutEventHandler):
    """ActiveFS
    Currently, we assume that only a single job is submitted
    """
    def __init__(self, ev, config):
        self.ev = ev
        self.config = config
        self.osds = [ ActiveFlash(ev, n, self) \
                        for n in range(self.config.osds) ]

        """We also use the host for computation?
        if self.config.hybrid == True:
            self.host = ActiveHost(ev, 0, self)
        else:
            self.host = None
        """
        self.host = None

        self.ev.register_module(self)
        self.pq = []    # pre(pared) q, all data files are ready

        if self.config.scheduler == 'locality':
            self.scheduler = sched.SchedLocality(self)
        elif self.config.scheduler == 'minwait':
            self.scheduler = sched.SchedMinWait(self)
        elif self.config.scheduler == 'hostonly':
            self.scheduler = sched.SchedHostOnly(self)
            self.set_hybrid()
        elif self.config.scheduler == 'hostreduce':
            self.scheduler = sched.SchedHostReduce(self)
            self.set_hybrid()
        else:
            self.scheduler = sched.SchedRR(self)

    def set_hybrid(self):
        self.host = ActiveHost(self.ev, 0, self)

    def get_name(self):
        return 'ActiveFS'

    def submit_job(self, js):
        """deprecated, use the submit_workflow()
        """
        try:
            self.job = job.ActiveJob(js)
        except:
            raise

        self.populate_files()
        self.tq = list(self.job.tasks.values())
        self.scheduler.job_submitted()

    def submit_workflow(self, root):
        """Nasty conversion from xml to json
        """
        ns = { 'ns':'http://pegasus.isi.edu/schema/DAX' }
        js = {}
        js['files'] = {}
        js['tasks'] = {}

        jobs = root.findall('ns:job', namespaces=ns)
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

        # after conversion, call submit_job()
        self.submit_job(js)

    def populate_files_random(self):
        pass

    def populate_files(self):
        if self.config.placement == 'explicit':
            return

        sorted_files = list(map(lambda x: x[1],
                            sorted(self.job.files.items())))
        valid_files = list(filter(lambda x: x.size > 0, sorted_files))

        if self.config.placement == 'random':
            #self.populate_files_random()
            li = range(self.config.osds)
            random.shuffle(li)
            for (f, osd) in zip(valid_files, cycle(li)):
                f.set_location(osd)
        else:   # including 'rr', others fallback here
            for (f, osd) in zip(valid_files,
                                cycle(range(self.config.osds))):
                f.set_location(osd)

    def task_completed(self, task, osd):
        self.advance()

    def check_termination(self):
        if len(self.tq) + len(self.pq) == 0:
            return True
        else:
            return False

    def request_data_transfer(self, task):
        transfer_from = [ 0 for x in range(len(self.osds)) ]
        transfer_list = [ task.osd ]    # first element is the destination
        for f in task.input:
            if not f.is_replicated(task.osd):
                transfer_from[f.location] += f.size
                task.account_transfer(f)
                transfer_list += [ f ]  # rests are the files

        transfer_total = reduce(lambda x, y: x+y, transfer_from)
        if transfer_total > 0:
            """the exact estimation??? this is a very conservative approach,
            which considers the worst case.
            updated on 3/3/2014: this calculation is too simplified. actually,
            replicating a file involves host system to intervene; host
            filesystem should move the file by reading it into its memory and
            write it back to a desired osd. it can be seen, as the worst case,
            that the file transfers work serially.
            """
            # the following old calculation is replaced.
            #delay = 3 * (float(max(transfer_from)) / self.config.netbw)
            #delay = 0.5 + 2 * (float(transfer_total) / self.config.netbw)
            """updated on 3/24/2014: this will catch the pattern of the reduce,
            but not broadcast.
            """
            delay = reduce(lambda x, y: x+y,
                           map(lambda x: 2.0 * (0.3 + \
                                       float(x.size)*1.02 / self.config.netbw),
                               transfer_list[1:]))

            #print 'transfer delay: %f' % delay
            e = event.TimeoutEvent('transfer', delay, self)

            if len(transfer_list) == 2: # single transfer, set source
                e.source = transfer_list[1].location

            e.set_context(transfer_list)
            desc = '{}({}) transfers {}'. \
                    format(task.name, task.osd,
                           map(lambda x: (x.name, x.size, x.location), \
                               transfer_list[1:]))
            e.set_description(desc)
            self.ev.register_event(e)

    def handle_transfer_complete(self, e):
        li = e.get_context()
        osd = li[0]
        for f in li[1:]:
            f.add_replica(osd)
            """update the rw statistics"""
            self.osds[osd].data_transfer_write(f.size)
            self.osds[f.location].data_transfer_read(f.size)

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
                if task.host == False:
                    self.request_data_transfer(task)

    def handle_ready_tasks(self):
        ready = [ task for task in self.pq if task.is_ready() ]
        if len(ready) > 0:
            for task in self.pq[:]:
                if task in ready:
                    self.pq.remove(task)

            for task in ready:
                if task.host == True:
                    self.host.submit_task(task)
                else:
                    self.osds[task.osd].submit_task(task)

    def handle_timeout(self, e):
        if self.config.eventlog:
            print '(%.3f, %.3f) --- %s [%s] %s' % \
                  (e.registered, e.timeout, self.get_name(),
                   e.name, e.description)

        if e.name == 'transfer':
            self.handle_transfer_complete(e)
        else:
            pass

        self.advance()


