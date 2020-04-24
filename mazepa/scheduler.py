import time
import uuid

from .queue import Queue
from .job_ready import AllJobsReady

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
        jobs_ready = AllJobsReady()
        self.submit_jobs(jobs_ready)

        while self.unfinished_jobs:
            self.submit_ready_jobs()
            time.sleep(sleep_gap_sec)

    def submit_jobs(self, jobs_ready):
        tasks = []
        jobs_just_finished = []
        # Get next round of tasks for all unfinished jobs
        for job_name, job_task_generator in six.iteritems(self.unfinished_jobs):
            if isinstance(jobs_ready, AllJobsReady) or \
                    job_name in jobs_ready:
                try:
                    this_job_tasks = job_task_generator()
                except StopIteration:
                    jobs_just_finished.append(job_name)
                else:
                    for t in this_job_tasks:
                        tasks.tags['job_name'] = job_name
                    tasks.extend(this_job_tasks)

        # Move finished jobs to finished dict
        for job_name in jobs_just_finished:
            self.finished_jobs[job_name] = self.unfinished_jobs[job_name]
            del self.unfinished_jobs[job_name]

        # Send the tasks and wait till (some) are done
        self.queue.submit(tasks)


    def register_job(self, job_task_generator, job_name=None):
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
