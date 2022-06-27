import types

class Task:
    def __init__(self, *args, **kwargs):
        self.job_id = None

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class Job:
    def __init__(self, *args, task_batch_size=10000, **kwargs):
        self.task_batch_size = task_batch_size
        self.task_generator = self.task_generator()
        self.task_batch_generator = self.task_batch_generator()
        self.waiting_for_completion = False

    def get_tasks(self):
        return next(self.task_generator)

    def task_generator(self):
        raise NotImplemented("Jobs must implement 'task_generator' function")

    def get_task_batch(self):
        return next(self.task_batch_generator)

    def task_batch_generator(self):
        '''
        yields: list of tasks that can be completed. when done, yields an empty list
        '''
        result = []

        while True:
            #squeeze the job until it's either done or returns a barrier, or reaches the max number of tasks per batch
            try:
                task_batch = self.get_tasks()
                if task_batch is Barrier:
                    if len(result) == 0 and self.waiting_for_completion:
                        raise Exception(f"Job '{type(self)}' issued two Barriers in a row, "
                                         "or issued a barrier as the first task")
                    else:
                        self.waiting_for_completion = True
                        yield result
                        result = []
                elif isinstance(task_batch, list):
                    result.extend(task_batch)
                elif isinstance(task_batch, types.GeneratorType):
                    for new_task in task_batch:
                        result.append(new_task)
                        if len(result) >= self.task_batch_size:
                            self.waiting_for_completion = False
                            yield result
                            result = []
                else:
                    raise Exception(f"Object of unsupported type '{type(task_batch)}' yielded task_generator")

                if len(result) >= self.task_batch_size:
                    self.waiting_for_completion = False
                    yield result
                    result = []

            except StopIteration:
                #means that this job has no more tasks, it's done
                yield result
                break


        self.waiting_for_completion = True
        yield []
        yield None

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

