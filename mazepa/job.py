class Task:
    def __init__(self, *kargs, **kwargs):
        self.job_id = None

    def __call__(self, *kargs, **kwargs):
        raise NotImplementedError


class Job:
    def __init__(self, *kargs, **kwargs):
        pass

    def get_tasks(self):
        raise NotImplementedError

    def get_next_task_batch(self):
        '''
        returns: list of tasks that can be completed. when done, returns an empty list
        '''
        result = []
        while True:
            #squeeze the job until it's either done or returns a barrier
            try:
                next_step = self.get_tasks()
            except StopIteration:
                #means that this job has no more tasks, it's done
                break
            else:
                if next_step is Barrier:
                    if len(result) == 0:
                        raise Exception(f"Job '{type(self)}' issued two Bariers in a row")
                    break
                elif isinstance(next_step, list):
                    result.extend(next_step)

        return result

    def old_get_next_task_batch(self):
            '''
            Main execution loop.
            Returns type of next(self.iterator) must be either a lits (of tasks),
            execution signal, or another Mazepa Job. When the job is done,
            next(self.iterator)) will raise a StopIteration exception.

            Results are accumulated until one of two conditions is met:
                1)  next(self.iterator)) raises a StopIteration exception, meaning
                   that this job is complete, and the no_more_tasks flag is set
                   to indicate completion.
                2) next(self.iterator) returns an execution signal of type Barrier,
                   which means that proceeding tasks need to wait for current
                   tasks to complete first.
            '''
            while True:
                try:
                    if isinstance(self.next_step, MazepaExecutionSignal):
                        if isinstance(self.next_step, Barrier):
                            self.next_step = next(self.iterator)
                            return result
                        else:
                            raise Exception("Unknown execution signal \
                                    of type {}".format(type(self.next_step)))

                    elif isinstance(self.next_step, Job):
                        subjob = self.next_step
                        subjob_task_batch = subjob.get_next_task_batch()
                        result += subjob_task_batch

                        if subjob.has_more_tasks():
                            # if the subjob is not done, it means theres
                            # a dependency inside it, ie it received a Barrier.
                            # have to wait until current tasks complete
                            # the next iteration of the while loop will come back
                            # to call get_next_task_batch on this subjob
                            return result
                        else:
                            self.next_step = next(self.iterator)

                    else:
                        task_list = self.next_step
                        result += task_list
                        self.next_step = next(self.iterator)

                except StopIteration:
                   self.no_more_tasks = True
                   return result

class MazepaExecutionSignal:
    def __init__(self):
        pass

class Barrier(MazepaExecutionSignal):
    def __init__(self):
        pass

class JobReadyIndicator:
    def __init__(self):
        pass

class JobReadyList(JobReadyIndicator):
    def __init__(self, job_list):
        super().__init__()
        self.job_list = job_list

class AllJobsIndicator(JobReadyIndicator):
    def __init__(self):
        super().__init__()

