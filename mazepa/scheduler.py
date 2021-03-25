import time
import uuid
import six

from mazepa.queue import Queue
from mazepa.job import Job, AllJobsIndicator

class Scheduler:
    def __init__(self, queue_name=None, completion_queue_name=None,
            queue_region=None, threads=8):
        self.queue = Queue(queue_name=queue_name,
                completion_queue_name=completion_queue_name,
                queue_region=queue_region,
                threads=threads)

        self.unfinished_jobs = {}
        self.finished_jobs   = {}

    def all_jobs_finished(self):
        return bool(self.unfinished_jobs)

    def submit_ready_jobs(self):
        jobs_ready = self.queue.get_completed()
        if jobs_ready is not None:
            self.submit_jobs(jobs_ready)

    def execute_until_completion(self, sleep_gap_sec=4):
        jobs_spec = AllJobsIndicator()
        self.submit_jobs(jobs_spec)

        while True:
            self.submit_ready_jobs()
            if not self.unfinished_jobs:
                break
            time.sleep(sleep_gap_sec)

    def submit_jobs(self, jobs_spec):
        tasks = []
        jobs_just_finished = []

        for job_name, job in six.iteritems(self.unfinished_jobs):
            this_job_tasks = []
            # if this job is flagged for execution
            if isinstance(jobs_spec, AllJobsIndicator) or \
                    job_name in jobs_spec:
                if isinstance(job, Job):
                    this_job_tasks = job.get_task_batch()
                    print ("Got {} tasks from job '{}'".format(len(this_job_tasks),
                            job_name))
                    if this_job_tasks == []:
                        jobs_just_finished.append(job_name)
                else:
                    try:
                        this_job_tasks = job()
                    except StopIteration:
                        print ("Job '{}' is done!")
                        jobs_just_finished.append(job_name)
                for t in this_job_tasks:
                    t.job_name = job_name

                tasks.extend(this_job_tasks)

        # Move finished jobs to finished dict
        for job_name in jobs_just_finished:
            print ("Flagging job as FINISHED: '{}'".format(job_name))
            self.finished_jobs[job_name] = self.unfinished_jobs[job_name]
            del self.unfinished_jobs[job_name]

        if len(tasks) > 0:
            print ("Scheduling {} tasks..".format(len(tasks)))
            self.queue.submit_mazepa_tasks(tasks)
        else:
            print ("No tasks to submit!")


    def register_job(self, job, job_name=None):
        '''
        job must be either an istance of type mazepa.Job
        or a simple generator
        '''
        if not isinstance(job, Job):
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
