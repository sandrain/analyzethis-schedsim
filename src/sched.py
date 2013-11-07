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
    """Input-Locality, a task is scheduled to osd where its largest input file
    is stored.
    """
    def task_prepared(self, ready_list):
        for task in ready_list:
            task.osd = reduce(lambda x, y: y if x.size < y.size else x,
                              task.input).location

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
                task.osd = reduce(lambda x, y: y if x.size < y.size else x,
                                  task.input).location



