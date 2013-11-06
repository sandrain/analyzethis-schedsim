#!/usr/bin/env python

from itertools import *
import activefs

class Scheduler:
    """Interface of activefs scheduler
    """
    def __init__(self, afs):
        self.afs = afs

    def job_submitted(self):
        pass

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
                               cycle(range(self.afs.config.n_osds))):
            task.osd = osd


class SchedInput(Scheduler):
    """Input-Locality, a task is scheduled to osd where its largest input file
    is stored.
    """
    pass

