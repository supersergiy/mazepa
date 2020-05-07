import time
import uuid

from mazepa.queue import Queue
from mazepa.job import Job
from mazepa.serialization import serialize_task_batch

class Scheduler:
    def __init__(self, queue_name=None, threads=1):
        self.queue = Queue(queue_name=queue_name, threads=threads)
        self.unfinished_jobs = {}
        self.finished_jobs   = {}

    def all_jobs_finished(self):
        return bool(self.unfinished_jobs)

    def submit_ready_jobs(self):
        jobs_ready = self.queue.get_completed()
        if jobs_ready is not None:
            self.submit_jobs(jobs_ready)

    def execute_until_completion(self, sleep_gap_sec=15):
        jobs_spec = AllJobs()
        self.submit_jobs(jobs_spec)

        while self.unfinished_jobs:
            self.submit_ready_jobs()
            time.sleep(sleep_gap_sec)

    def submit_jobs(self, jobs_spec):
        tasks = []
        jobs_just_finished = []

        for job_name, job in six.iteritems(self.unfinished_jobs):
            this_job_tasks = []
            # if this job is flagged for execution
            if isinstance(jobs_spec, AllJobs) or \
                    job_name in jobs_spec:
                if isinstance(job, mazepa.Job):
                    if job.no_more_tasks:
                        jobs_just_finished.append(job_name)
                    else:
                        this_job_tasks = job.get_next_task_batch()
                else:
                    try:
                        this_job_tasks = job()
                    except StopIteration:
                        jobs_just_finished.append(job_name)

                for t in this_job_tasks:
                    tasks.tags['job_name'] = job_name
                tasks.extend(this_job_tasks)

        # Move finished jobs to finished dict
        for job_name in jobs_just_finished:
            self.finished_jobs[job_name] = self.unfinished_jobs[job_name]
            del self.unfinished_jobs[job_name]

        self.queue.submit_mazepa_tasks(tasks)


    def register_job(self, job, job_name=None):
        '''
        job must be either an istance of type mazepa.Job
        or a simple generator
        '''
        if not isinstance(job, mazepa.Job):
           if not inspect.isgeneratorfunction(job) and \
                   not inspect.isgenerator(job):
                       raise Exception("Registering job that is \
                               neither Mazepa job nor a generator is\
                               not supported. Submitted job type: {}".format(
                                   type(job)
                                   ))
        if job_name is None:
            job_name = uuid.uuid1()
            while job_name in self.unfinished_jobs:
                job_name = uuid.uuid1()
        else:
            if job_name in self.unfinished_jobs:
                raise Exception('Unable to register task "{}": \
                        task with name "{}" is already registerd'.format(job_name, job_name))

        self.unfinished_jobs[job_name] = job

        return job_name

    def unregister_job(self, job_name):
        del self.unfinished_jobs[job_name]
