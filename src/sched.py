#!/usr/bin/env python

from itertools import *
from functools import reduce
import activefs

class Scheduler:
    """Interface of activefs scheduler
    """
    def __init__(self, afs):
        self.afs = afs

    def job_submitted(self):
        pass

    """all input files are created, will be transferred according to the
    scheduling decision.
    @ready_list: list of tasks ready
    """
    def task_prepared(self, ready_list):
        pass

    def task_completed(self, task):
        pass


class SchedRR(Scheduler):
    """Basic round-robin scheduler
    """
    def job_submitted(self):
        sorted_tasks = list(map(lambda x: x[1],
                            sorted(self.afs.job.tasks.items())))
        for (task, osd) in zip(sorted_tasks,
                               cycle(range(self.afs.config.osds))):
            task.osd = osd


class SchedInput(Scheduler):
    def find_osd(self, task):
        fsize = [0] * self.afs.config.osds
        for f in task.input:
            fsize[f.location] += f.size
        return fsize.index(max(fsize))

    """Input-Locality, a task is scheduled to osd where its largest input file
    is stored.

    hyogi: the above calculation is wrong, but we need to find the osd where
    majority of input files are stored.
    """
    def task_prepared(self, ready_list):
        for task in ready_list:
            task.osd = self.find_osd(task)

        """the old way, which is wrong"""
        """
        for task in ready_list:
            task.osd = reduce(lambda x, y: y if x.size < y.size else x,
                              task.input).location
        """

class SchedInputEnhanced(Scheduler):
    """Enhanced Input-Locality scheduler. The problem of the naive input
    scheduler is that it cannot utilize the osds when initially input files are
    stored in only subset of them. The enhancement here is: if width of some
    level (in the workflow tree) is greater than the number of osds which store
    the input file, then the tasks are assigned in a round-robin fashion.

    For the implementation, we need to figure out 'how many read tasks are
    dependent on a certain file?'. If the number of such tasks are large
    enough or runtime of those tasks are significantly long enough to beat the
    data transfer time, we spread the tasks, which will result in data
    transfer to other active flashes. This especially works for broadcasting
    patterns and it is very likely that we can catch such opportunities in the
    task_prepared function, because such tasks would become ready once the
    file, which will be broadcated, is generated.
    """
    def task_prepared(self, ready_list):
        """get the list of input files"""
        nfiles = len(list(set(reduce(lambda x,y: x+y,
                                [ task.input for task in ready_list ]))))
        """if the size is larger than the number of tasks in readylist,
        spread the tasks"""
        if len(ready_list) > 2 * nfiles:
            sorted_tasks = sorted(ready_list, key=lambda x: x.name)
            for (task, osd) in zip(sorted_tasks,
                                   cycle(range(self.afs.config.osds))):
                task.osd = osd
        else:
            for task in ready_list:
                osd = 0
                if len(task.input) > 0:
                    task.osd = reduce(lambda x, y: y if x.size < y.size else x,
                                      task.input).location
                else:
                    if osd == self.afs.config.osds:
                        osd, task.osd = 0, 0
                    else:
                        task.osd = osd
                        osd = osd + 1


class SchedMinWait(Scheduler):
    def task_prepared(self, ready_list):
        """ calculate expected wait time for all osds
        """
        wait = [ 0.0 ] * self.afs.config.osds

        """get the queue population
        """
        for i in range(self.afs.config.osds):
            if len(self.afs.osds[i].tq) > 0:
                wait[i] = self.afs.osds[i].get_qtime()

        """ assign osds for tasks
        """
        for task in ready_list:
            """get the file distribution
            """
            fsize = [0] * self.afs.config.osds
            for f in task.input:
                fsize[f.location] += f.size
            fsize_total = reduce(lambda x,y: x+y, fsize)
            for i in range(self.afs.config.osds):
                wait[i] = wait[i] + \
                    (float(fsize_total - fsize[i]) / self.afs.config.netbw)

            """select the minimum wait time one
            """
            osd = wait.index(min(wait))
            task.osd = osd

            """need to update the wait time
            """
            wait = [ 0.0 ] * self.afs.config.osds
            wait[osd] += task.runtime   # consider the current task
            for i in range(self.afs.config.osds):
                if len(self.afs.osds[i].tq) > 0:
                    wait[i] = self.afs.osds[i].get_qtime()


class SchedHostOnly(Scheduler):
    def task_prepared(self, ready_list):
        for task in ready_list:
            task.host = True

class SchedHostReduce(Scheduler):
    def task_prepared(self, ready_list):
        """The scheduling is based on the minwait
        """
        wait = [ 0.0 ] * self.afs.config.osds
        for i in range(self.afs.config.osds):
            if len(self.afs.osds[i].tq) > 0:
                wait[i] = self.afs.osds[i].get_qtime()

        for task in ready_list:
            osd_list = []
            for f in task.input:
                osd_list += [ f.location ]
            osd_list = list(set(osd_list))

            """This should be passed as argument (0.5)
            If the files are spreaded more than 50% of the available osds
            schedule the task to the host
            """
            if len(osd_list) > self.afs.config.osds * 0.5:
                task.host = True
                return

            """fall back to minwait
            """
            fsize = [0] * self.afs.config.osds
            for f in task.input:
                fsize[f.location] += f.size
            fsize_total = reduce(lambda x,y: x+y, fsize)
            for i in range(self.afs.config.osds):
                wait[i] = wait[i] + \
                    (float(fsize_total - fsize[i]) / self.afs.config.netbw)

            """select the minimum wait time one
            """
            osd = wait.index(min(wait))
            task.osd = osd

            """need to update the wait time
            """
            wait = [ 0.0 ] * self.afs.config.osds
            wait[osd] += task.runtime   # consider the current task
            for i in range(self.afs.config.osds):
                if len(self.afs.osds[i].tq) > 0:
                    wait[i] = self.afs.osds[i].get_qtime()

