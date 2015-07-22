#!/usr/bin/env python

from itertools import *
from functools import reduce
import activefs

""" Code for the scheduling at the device level (multi-core devices)
"""
class DeviceScheduler:
    # A class specific constants
    (SCHED_LOOP_DONE, SCHED_LOOP_CONT) = (-1, -2)
    def __init__(self):
        self.init = True

class DeviceSchedFirstFreeCore(DeviceScheduler):
    def schedule_task(self, device, task):
        for i in range(device.num_cores):
            if device.cores[i].running != None:
                continue
    
            # If a core is not running any task, we transfer a task
            # to the core's tq
            if (len(device.tq) > 0 and device.cores[i].running == None):
                return i

        # If we reach this point, it means that we could not assign any
        # task and the scheduler loop terminated
        return self.SCHED_LOOP_DONE

""" Code for the scheduling across multiple devices
"""

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

class SchedWA(Scheduler):
    """Scheduling policy aiming at decreasing the write amplification ratio
    """
    def get_task_total_input_size(self, task):
        """ Simple function that returns the total size of all the input
            files for a given task
        """
        total_input_size = 0
        for f in task.input:
            total_input_size += f.size
        print 'Total size of the input files: ', total_input_size, ' - task: ', task.name
        return total_input_size

    def size_required_data_transfer(self, task, afe):
        """ Function that return the write amplification ratio for a given placement of
            a task on a target AFE
        """
        print 'Calculating the amount of data if assigning task to AFE ', afe.id
        transfer = 0
        for f in task.input:
            print '\tCurrent location of file: ', f.name, ': ', f.location, ' - Potential target: ', afe.id
            if f.location != afe.id:
                transfer += f.size
        print 'Total size of files to transfer: ', transfer, ' - task: ', task.name, '  - AFE ID: ', afe.id
        return transfer

    def get_task_wa_for_afe(self, task, afe):
        """ Function that returns the write amplification ratio if the task
            is assigned to a target AFE
        """
        data_moved = self.size_required_data_transfer(task, afe)
        total_data = self.get_task_total_input_size(task)
        wa = 1.000 * data_moved / total_data
        print 'WA ratio for assigning task ', task.name, 'to AFE ', afe.id, ': ', wa, '(', data_moved, '/', total_data, ')'
        return wa

    def get_task_min_wa(self, task, afes):
        """ Get the lowest write amplification ratio when concidering allocating a
            task on a set of AFEs
        """
        min_wa = float(-1)
        select_afe_index = 0
        for i in range(len(afes)):
            wa = self.get_task_wa_for_afe(task, afes[i])
            if (min_wa == -1):
                min_wa = wa
                select_afe_index = i
            elif (wa < min_wa):
                min_wa = wa
                select_afe_index = i
        print '\n\n'
        return min_wa, select_afe_index

    def task_prepared(self, ready_list):
        afes = list(self.afs.osds)
        tasks = list(ready_list)

        """ All tasks must be placed in the context of the execution of this function
            because of a current implementation limitation. Because of that, we calculate
            how many tasks must be assigned to the different AFEs so that the same number
            of tasks is assigned to each AFE. This may not be the most efficient placement
            since we do not take into account to execution time of a task but we currently
            focus only decreasing the Write Amplification and still have a parallel
            execution of tasks
        """
        tasks_per_afe = len(tasks) / len(afes)
        print tasks_per_afe, " tasks must be assigned to each AFE"
        j = 0
        while len(afes) > 0 and len(tasks) > 0:
            print len(tasks), " tasks candidate for placement on ", len(afes), " AFEs"

            i = 0
            min_wa = -1
            select_task_index = 0
            select_afe_index = 0
            select_task_name = ''
            select_afe_id = -1
            afes_assigned_tasks = [0]*len(afes)

            while (i < len(tasks)):
                task_wa, afe_index = self.get_task_min_wa(tasks[i], afes)
                if min_wa == -1:
                    min_wa = task_wa
                    select_task_index = i
                    select_afe_index = afe_index
                    select_task_name = tasks[i].name
                    if (select_afe_index < len(afes)):
                        select_afe_id = afes[select_afe_index].id
                    else:
                        print 'Error: index ', select_afe_index, 'is outside of array (size: ', len(afes), ')'
                        return
                elif (task_wa < min_wa):
                    min_wa = task_wa
                    select_task_index = i
                    select_afe_index = afe_index
                    select_afe_id = afes[select_afe_index].id
                    select_task_name = tasks[i].name
                i = i + 1

            print '\n\nLoop ', j, 'task ', select_task_name, '(index: ', select_task_index, ') is selected to be executed on AFE ', select_afe_id
            print '\n\n'

            """ We update the placement of the select task
            """
            print 'Select AFE ID: ', select_afe_id, ' - task index: ', select_task_index
            index_target_osd = 0
            while (self.afs.osds[index_target_osd].id != select_afe_id):
                index_target_osd = index_target_osd + 1

            i = 0
            while (ready_list[i].name != select_task_name):
                i = i + 1
            ready_list[i].osd = index_target_osd
            
            """ We remove the task from the local list of tasks that
                need to be scheduled
            """
            if (select_task_index < len(tasks)):
                tasks.pop(select_task_index)
            else:
                print 'Error: index ', select_task_index, ' is outside of array (size: ', len(tasks), ')'

            """ We save the fact that the AFE has one more task assigned
            """
            afes_assigned_tasks[select_afe_index] = afes_assigned_tasks[select_afe_index] + 1
            """ If the max of tasks has been assigned to the AFE, we remove it
                from the local list of available AFEs
            """
            if (len(afes_assigned_tasks) > 1 and afes_assigned_tasks[select_afe_index] == tasks_per_afe):
                print "Marking AFE ", select_afe_index, "as busy"
                afes.pop(select_afe_index)

            j = j + 1
        print "\n\n\n*********************************\n\n\n"

class SchedRR(Scheduler):
    """Basic round-robin scheduler
    """
    def job_submitted(self):
        sorted_tasks = list(map(lambda x: x[1],
                            sorted(self.afs.job.tasks.items())))
        for (task, osd) in zip(sorted_tasks,
                               cycle(range(self.afs.config.osds))):
            task.osd = osd


class SchedLocality(Scheduler):
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

