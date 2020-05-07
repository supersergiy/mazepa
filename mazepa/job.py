from serialization import serializable

@serializable
class Task:
    def __call__(self, *kargs, **kwargs):
        raise NotImplementedError

class Job:
    def __int__(self):
        self.is_completed = False

    def __call__(self, *kargs, **kwargs):
        raise NotImplementedError

    def get_next_task_batch(self):
        '''
        returns: list of tasks, possibly empty
        '''
        result = []

        while True:
            '''
            Main execution loop.
            Return type of self.__call__() must be either a lits (of tasks),
            execution signal, or another Mazepa Job. When the job is done,
            self.__call__() will raise a StopIteration exception.

            Results are accumulated until one of two conditions is met:
                1) self.__call__() raises a StopIteration exception, meaning
                   that this job is complete, and the is_completed flag is set
                   to indicate completion.
                2) self.__call__() returns an execution signal of type Barrier,
                   which means that proceeding tasks need to wait for current
                   tasks to complete first.
            '''

            next_step = self.__call__()
            while True:
                try:
                    if isinstance(next_step, MazepaExecutionSignal):
                        if isinstance(next_step, Barrier):
                            yield result
                            result = []
                            next_step = self.__call__()
                        else:
                            raise Exception("Unknown execution signal \
                                    of type {}".format(type(next_step)))
                    elif isinstance(next_step, MazepaJob):
                        subjob = next_step
                        subjob_task_batch = next_step.get_next_task_batch()
                        result += subjob_task_batch

                        if not subjob.is_completed:
                            # if the subjob is not done, it means theres
                            # a dependency inside it, ie it received a Barrier.
                            # have to wait until current tasks complete
                            # the next iteration of the while loop will come back
                            # to call get_next_task_batch on this subjob
                            yield result
                            result = []

                    else:
                        task_list = next_step
                        retult += task_list
                        next_step = self.__call__()

                except StopIteration:
                   self.is_completed = True
                   yield result
                   break

            raise Exception("A completed job is polled for tasks. \
                    self.is_completed value == {}".format(self.is_completed))

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
        super().__init__(self)
        self.job_list = job_list

class AllJobsIndicator(JobReadyIndicator):
    def __init__(self):
        super().__init__(self)

