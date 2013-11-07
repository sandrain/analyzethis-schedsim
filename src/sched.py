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


