#!/usr/bin/env python

from itertools import *
from functools import reduce
import random
import job
import event
import sched
import logging
import iomodel

bin_map = dict({    "fits.tbl":3,
                    "mAdd":1,
                    "mBgExec":3,
                    "mBgModel":0,
                    "mDiffFit":3,
                    "mImgtbl":1,
                    "mJPEG":2,
                    "mOverlaps":3,
                    "mProjectPP":2  })

class AFECore(event.TimeoutEventHandler):
    """afe core
    """
    def __init__(self, ev, core_id, activeflash, afs):
        # activeflash allows us to have a reference to the representation
        # of ActiveFlash hosting this core
        self.activeflash = activeflash
        self.ev = ev
        self.core_id = core_id
        self.afs = afs
        self.tq = []
        self.n_read = 0
        self.n_written = 0
        self.running = None
        self.idle_event = event.TimeoutEvent('idle', 1, self)
        self.idle_event.set_disposable()
        self.task_event = event.TimeoutEvent('task', 0, self)
        self.ev.register_module(self)
        logging.basicConfig (level=logging.DEBUG,
                             format='%(asctime)s - %(levelname)s - %(message)s')
        logging.basicConfig (level=logging.INFO,
                             format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger (__name__)
        if self.afs.config.eventlog == 'debug':
            self.logger.setLevel (logging.DEBUG)
        elif self.afs.config.eventlog == 'info':
            self.logger.setLevel (logging.INFO)
        else:
            self.logger.propagate = False

    def get_name(self):
        return 'Core-' + str(self.core_id)

    def get_state(self):
        return self.get_name()

    def try_execute_task(self):
        """ Function that tries to execute a new task from the task queue
        """

        # If an event is already running, we just pass
        if self.running != None:
            return

        if len(self.tq) > 0:
            # We get the first task from the task queue
            task = self.tq.pop(0)
            # We mark the task as being the one executed on the core
            self.running = task

            desc = '%s (%.3f sec) execution' % (task.name, task.runtime)

            # We set the execution start time of the task
            task.started(self.ev.now())

            # We set an event that will simulate the task termination
            self.task_event.set_timeout(task.runtime)
            self.task_event.set_context(task)
            self.task_event.set_description(desc)
            self.ev.register_event(self.task_event)

            # We set an event at the activeflash device level so it can get
            # the task termination notification
            self.activeflash.task_event.set_timeout(task.runtime)
            self.activeflash.task_event.set_context(task)
            self.activeflash.task_event.set_description(desc)
            self.ev.register_event(self.activeflash.task_event)

    def submit_task(self, task):
        self.tq.append(task)
        # Submission time is when the task is added to the meta queue at the
        # activeflash device level so nothing to do to that regard here
        self.try_execute_task()

    def handle_timeout(self, e):
        if self.afs.config.eventlog:
            self.logger.debug ('(%.3f, %.3f) --- %s [%s] %s' \
                  % (e.registered, e.timeout, self.get_name(),
                     e.name, e.description))

        if e.name == 'task':
            self.handle_timeout_task(e)

    """Handles task completion event
    """
    def handle_timeout_task(self, e):
        task = e.get_context()
        if (task == None):
            raise SystemExit('BUG')
        self.logger.debug ("Task %s terminated" % task.name)
        # We set the execution end time of the task
        task.completed(self.ev.now())
        # Run one execution iteration of the simulator (task_completed does
        # nothing but execute advance())
        self.afs.task_completed(task, self)
        # We update the core's performance metrics based on the execution of
        # the task
        self.update_data_rw(task)
        # We mark the core as not running any task
        self.running = None
        # We see if another task can be executed
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


class ActiveFlash(event.TimeoutEventHandler):
    """Active flash element
    """
    def __init__(self, ev, id, afs):
        self.id = id
        self.ev = ev
        self.afs = afs
        self.tq = []
        self.cores = []
        self.ev.register_module(self)
        self.idle_event = event.TimeoutEvent('idle', 1, self)
        self.idle_event.set_disposable()
        self.task_event = event.TimeoutEvent('task', 0, self)
        """statistics"""
        self.n_read = 0         # how much is read/written?
        self.n_written = 0
        self.n_extra_read = 0   # how much rw for data transfer?
        self.n_extra_written = 0
        self.num_cores = self.afs.config.cores

        logging.basicConfig (level=logging.DEBUG,
                             format='%(asctime)s - %(levelname)s - %(message)s')
        logging.basicConfig (level=logging.INFO,
                             format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger (__name__)
        if self.afs.config.eventlog == 'debug':
            self.logger.setLevel (logging.DEBUG)
        elif self.afs.config.eventlog == 'info':
            self.logger.setLevel (logging.INFO)
        else:
            self.logger.propagate = False

        ## ID of the core that will be used for the assignement of
        ## the next task to run
        #self.next_core = 0
        # Initialize the device's cores
        for i in range(self.num_cores):
            core = AFECore (ev, i, self, afs)
            self.cores.append (core)
        for i in range(len(self.cores)):
            core = self.cores[i]
        # Initialize the device's scheduler
        if (afs.config.deviceScheduler == 'firstavailable'):
            self.device_scheduler = sched.DeviceSchedFirstFreeCore()
        else:
            self.device_scheduler = sched.DeviceSchedFirstFreeCore()

    def get_name(self):
        return 'ActiveFlash-' + str(self.id)

    def get_state(self):
        s = ""
        for i in range(self.num_cores):
            s += "%s . %s" % (self.get_name(), self.cores[i].get_state())
        return s

    def submit_task(self, task):
        self.tq.append(task)
        task.submitted(self.ev.now())
        self.try_assign_task()

    def get_qtime(self):
        wait = 0.0
        for task in self.tq:
            wait += task.runtime
        return wait

    def set_idle_timeout(self):
        # The simulation will run only if events are present in the
        # queue. Unfortunatly, we may end up in a case where no new
        # event is created but tasks are present in the different
        # queues. To ensure the simulation won't stop when it should
        # not, we emit an idle event to guarantee progress
        # [GV] This is not required anymore! :)
        """
        self.idle_event.set_timeout(1)
        self.idle_event.set_context(None)
        self.idle_event.set_description(None)
        self.ev.register_event(self.idle_event)
        """

    def try_assign_task(self):
        # Find all the cores that are not running any tasks and
        # assign them a new task
        if (len(self.tq) == 0):
            # No task to schedule, we return
            return

        l = 0
        while (len(self.tq) > 0):
            # The scheduler is based on the following premise:
            # - we get the first task from the queue
            # - we try to scheduler the task on a core
            # - if the scheduler returns SCHED_LOOP_DONE, we stop
            # - if the scheduler returns SCHED_LOOP_CONT, the task cannot be
            #   assigned to a core but we can try to schedule another task
            # - if the scheduler returns a core ID, we assign the task to that core
            task = self.tq[l]

            # Call the scheduler
            core_id = self.device_scheduler.schedule_task(self, task)
            if (core_id == self.device_scheduler.SCHED_LOOP_DONE):
                # Scheduling loop ended, we exit
                # [GV] This is not required anymore.
                #self.set_idle_timeout()
                return
            elif (core_id == self.device_scheduler.SCHED_LOOP_CONT):
                # The task could not be assigned but we can try to assign next task
                l = l + 1
                continue
            else:
                task = self.tq.pop(l)
                self.cores[core_id].submit_task(task)

    # Handler executed when idle. Note that a core directly invoke that
    # function, not the event system.
    def handle_timeout(self, e):
        self.logger.debug ('(%.3f, %.3f) --- %s [%s] %s' \
                  % (e.registered, e.timeout, self.get_name(),
                     e.name, e.description))
        if e.name == 'task':
            self.handle_timeout_task(e)
        else:
            self.try_assign_task()

    """Handles task completion event
    """
    def handle_timeout_task(self, e):
        if (e == None):
            raise SystemExit('BUG')
        task = e.get_context()
        self.afs.task_completed(task, self)
        for f in task.output:
            if (f.size < 0):
                # The file is now produced, mark it accordingly
                f.size = -f.size
        self.update_data_rw(task)
        self.try_assign_task()

    def update_data_rw(self, task):
        if (task == None):
            raise SystemExit('BUG')
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
        self.workflow_runtime = 0.0
        self.last_ts = 0.0
        self.num_transfers = 0

        # Queue to store the file transfer requests
        self.fq = []

        # Simulate the latency of invoking the thread handling the transfer of
        # files.
        self.FILETRANSLATENCY = 0
        
        """
        File transfers are serialized to match the implementation of the
        emulator. This variable allows us to track whether a file transfer
        is already ongoing or not.
        """
        self.ongoing_file_transfer = False

        """We also use the host for computation?
        if self.config.hybrid == True:
            self.host = ActiveHost(ev, 0, self)
        else:
            self.host = None
        """
        self.host = None

        self.osds = [ ActiveFlash(ev, n, self) \
                        for n in range(self.config.osds) ]
        self.ev.register_module(self)
        self.pq = []    # pre(pared) q, all data files are ready

        logging.basicConfig (level=logging.DEBUG,
                             format='%(asctime)s - %(levelname)s - %(message)s')
        logging.basicConfig (level=logging.INFO,
                             format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger (__name__)
        if self.config.eventlog == 'debug':
            self.logger.setLevel (logging.DEBUG)
        elif self.config.eventlog == 'info':
            self.logger.setLevel (logging.INFO)
        else:
            self.logger.propagate = False

        self.iomod = iomodel.IOModel(config, "Emulator")

        if   self.config.scheduler == 'rr':
            self.scheduler = sched.SchedRR(self)
        elif self.config.scheduler == 'locality':
            self.scheduler = sched.SchedLocality(self)
        elif self.config.scheduler == 'minwait':
            self.scheduler = sched.SchedMinWait(self)
        elif self.config.scheduler == 'hostonly':
            self.scheduler = sched.SchedHostOnly(self)
            self.set_hybrid()
        elif self.config.scheduler == 'hostreduce':
            self.scheduler = sched.SchedHostReduce(self)
            self.set_hybrid()
        elif self.config.scheduler == 'wa':
            self.scheduler = sched.SchedWA(self)
        else:
            self.scheduler = sched.SchedLib(self)
        self.scheduler.config = config

    def set_hybrid(self):
        self.host = ActiveHost(self.ev, 0, self)

    def get_name(self):
        return 'ActiveFS'

    def submit_job(self):
        # The simulator is now ready to start to execute the workflow.
        self.last_ts = self.ev.now()
        self.logger.debug ('Initial TS: {0:.3f}'.format(self.last_ts))
        if (self.job == None):
            self.tq = []
        else:
            self.tq = list(self.job.tasks.values())
            self.scheduler.job_submitted()

    def submit_workflow(self, workflow):
        # after conversion, call submit_job()
        self.submit_job()

    def prepare_workflow(self, workflow):
        if workflow == None:
            self.job = None
        else:
            js = workflow.xmlToJson ()
            try:
                self.job = job.ActiveJob(js)
            except:
                raise

        self.populate_files()

    def populate_files_random(self):
        pass

    def populate_files(self):
#        if self.config.placement == 'explicit':
#            return

        # If not job is allocated, we simply exit
        if self.job == None:
            return

        sorted_files = list(map(lambda x: x[1],
                            sorted(self.job.files.items())))
        valid_files = list(filter(lambda x: x.size > 0, sorted_files))

        for f in valid_files:
            osd = self.config.py_lat_module.lat_host_sched_file()
            """
            GV: this code is used to mimic the emulator
            _osd = bin_map.get (f.name)
            if _osd != None:
                osd = _osd
            GV
            """
            f.set_location (osd)
            self.logger.info ("Placing file %s on AFE %d" % (f.name, osd))
        
#        if self.config.placement == 'random':
#            #self.populate_files_random()
#            li = range(self.config.osds)
#            random.shuffle(li)
#            for (f, osd) in zip(valid_files, cycle(li)):
#                f.set_location(osd)
#        else:   # including 'rr', others fallback here
#            for (f, osd) in zip(valid_files,
#                                cycle(range(self.config.osds))):
#                f.set_location(osd)

    # Update the run time based on the current time and the last time stamp
    def update_metrics(self):
        now = self.ev.now()
        self.workflow_runtime = self.workflow_runtime + (now - self.last_ts)
        self.last_ts = now

    def task_completed(self, task, osd):
        self.update_metrics()
        self.advance()

    def check_termination(self):
        # Does any of the cores still need to do some work?
        pending_work = 0
        for j in range(len(self.osds)):
            for i in range(self.osds[j].num_cores):
                pending_work += len(self.osds[j].cores[i].tq)
                if self.osds[j].cores[i].running != None:
                    pending_work = pending_work + 1
            pending_work = pending_work + len(self.tq) + len(self.pq)

        if pending_work == 0:
            self.logger.debug ("TERMINATED")
            return True
        else:
            return False

    def request_data_transfer(self, task):
        transfer_from = [ 0 for x in range(len(self.osds)) ]
        transfer_list = [ task.osd ]    # first element is the destination
        self.logger.debug ('Task %s has %d input files' % \
                            (task.name, len(task.input)))
        for f in task.input:
            if not f.is_replicated(task.osd):
                self.logger.info ("Request file transfer: %s from AFE %d to %d (task: %s, size: %d)" % (f.name, f.location, task.osd, task.name, f.size))
                transfer_from[f.location] += f.size
                task.account_transfer(f)
                self.num_transfers += 1
                self.fq.append ((task, f))
                e = event.TimeoutEvent ('filetransfreq', self.FILETRANSLATENCY, self)
                desc = '{}-{}: transfer request to {}'. \
                        format (task.name, f.name, task.osd) 
                e.set_description (desc)
                self.ev.register_event (e)

    def progress_file_transfers(self, evt):
        """
        File transfers are serialized. This function initiate a new file
        transfer (based on the queued file transfer requests) if there is no
        ongoing file transfer
        """
        if self.ongoing_file_transfer == False:
            self.logger.debug ("No ongoing file transfer")
            if (len (self.fq) > 0):
                self.ongoing_file_transfer = True
                (t, f) = self.fq.pop (0)
                #delay = 2.0 * (0.3 + float(f.size)*1.02 / self.config.netbw)
                delay = self.iomod.get_transfer_cost (f)
                e = event.TimeoutEvent('transfer', delay, self)
                e.set_context((t, f, delay))
                desc = 'Transfers {}({}) from {} to {} (time to transfer: {})'. \
                        format(f.name, f.size, f.location, t.osd, delay)
                self.logger.debug (desc)
                e.set_description(desc)
                self.ev.register_event(e)
            else:
                self.logger.debug ("No more pending file transfer request")
        else:
            self.logger.debug ("A file transfer is already ongoing, waiting...")

#                transfer_list += [ f ]  # rests are the files
#
#        # GV: This is totally wrong when dealing with multiple AFEs :(
#        transfer_total = reduce(lambda x, y: x+y, transfer_from)
#        n_osd = 0
#        for n in transfer_from:
#            if n > 0:
#                n_osd = n_osd + 1
#        print "Need to transfer files from %d AFEs for task %s" % (n_osd, task.name)
#        max_trans_time = 0.0
#        print "Need to transfer a total of %f bytes for task %s" % (transfer_total, task.name)
#        if transfer_total > 0:
#            """the exact estimation??? this is a very conservative approach,
#            which considers the worst case.
#            updated on 3/3/2014: this calculation is too simplified. actually,
#            replicating a file involves host system to intervene; host
#            filesystem should move the file by reading it into its memory and
#            write it back to a desired osd. it can be seen, as the worst case,
#            that the file transfers work serially.
#            """
#            # the following old calculation is replaced.
#            #delay = 3 * (float(max(transfer_from)) / self.config.netbw)
#            #delay = 0.5 + 2 * (float(transfer_total) / self.config.netbw)
#            """updated on 3/24/2014: this will catch the pattern of the reduce,
#            but not broadcast.
#            """
#            delay = reduce(lambda x, y: x+y,
#                           map(lambda x: 2.0 * (0.3 + \
#                                       float(x.size)*1.02 / self.config.netbw),
#                               transfer_list[1:]))
#
#            delay2 = reduce(lambda x, y: x+y,
#                           map(lambda x: (0.3 + \
#                                       float(x.size)*1.02 / self.config.netbw),
#                               transfer_list[1:]))
#
#            print 'Required transfer time for file %s, needed by task %s: %f' % (f.name, task.name, delay)
#            print 'TEST (%s): %f vs. %f' % (task.name, delay, delay2)
#            e = event.TimeoutEvent('transfer', delay, self)
#
#            if len(transfer_list) == 2: # single transfer, set source
#                e.source = transfer_list[1].location
#
#            e.set_context(transfer_list)
#            desc = '{}({}) transfers {}'. \
#                    format(task.name, task.osd,
#                           map(lambda x: (x.name, x.size, x.location), \
#                               transfer_list[1:]))
#            print "%s" % desc
#            e.set_description(desc)
#            self.ev.register_event(e)
#

    def handle_transfer_complete(self, e):
        (task, f, time) = e.get_context()
        self.logger.info ("File %s has been transfered in %.6f seconds" % \
                           (f.name, time))
        f.add_replica(task.osd)
        """update the rw statistics"""
        self.osds[task.osd].data_transfer_write(f.size)
        self.osds[f.location].data_transfer_read(f.size)

    def advance(self):
        self.handle_prepared_tasks()
        self.handle_ready_tasks()
        if self.check_termination():
            return

    def handle_prepared_tasks(self):
        for task in self.tq:
            if task.is_prepared():
                self.tq.remove (task)
                self.pq.append (task)
                self.scheduler.task_prepared (task)
                if (task.host == False):
                    self.request_data_transfer(task)
                e = event.TimeoutEvent ('prepare', 0, self)
                self.ev.register_event (e)
                break
        """
        Some file transfers may have completed since the last execution of the
        function so we check whether more tasks can transition to the
        'prepared' state
        """
        tasks = [ task for task in self.tq ]
        sorted_tasks = sorted (tasks, key=lambda task: task.name)
        unsorted_prepared = [ task for task in self.tq if task.is_prepared() ]
        prepared = sorted (unsorted_prepared, key=lambda task: task.name)
        self.logger.debug ("%d task(s) are prepared" % len (prepared))
        if len(prepared) > 0:
            """
            The queue 'tq' should only hold tasks that are submitted but
            not wait prepared/ready. So we need to make sure that the
            task that transition to the 'prepared' state are *not* in tq
            """
            #for task in sorted (self.tq[:]):
            for task in sorted_tasks:
                if task in prepared:
                    self.tq.remove(task)
            self.pq += prepared
            # We schedule the task to an AFE
            self.scheduler.task_prepared(prepared)

            """
            Now that we have all the task in the 'prepared' state, we check
            whether we can schedule a new file transfer.
            """
            for task in prepared:
                # task.host is used to easily check whether all the files
                # required to run the task are already available.
                if task.host == False:
                    self.logger.debug ("(%s) Checking input file..." % \
                                       task.name)
                    self.request_data_transfer(task)

    def handle_ready_tasks(self):
        ready = [ task for task in self.pq if task.is_ready() ]
        self.logger.debug ("%d tasks are ready" % len(ready))
        if len(ready) > 0:
            for task in self.pq[:]:
                if task in ready:
                    self.pq.remove(task)

            sorted_ready = sorted (ready, key=lambda task: task.name)
            for task in sorted_ready:
                if task.host == True:
                    self.host.submit_task(task)
                else:
                    self.logger.debug ("Submitting task %s" % task.name)
                    self.osds[task.osd].submit_task(task)

    def handle_timeout(self, e):
        if self.config.eventlog:
            self.logger.debug ('(%.3f, %.3f) --- %s [%s] %s' % \
                  (e.registered, e.timeout, self.get_name(),
                   e.name, e.description))

        if e.name == 'transfer':
            self.handle_transfer_complete(e)
            self.ongoing_file_transfer = False
            # A file transfer just completed, checking if more are required
            self.logger.debug ("Checking for more files to transfer...")
            self.progress_file_transfers (e)
        elif e.name == 'filetransfreq':
            self.progress_file_transfers (e)
        else:
            pass

        self.advance()


