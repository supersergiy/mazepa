import datetime
import json
import os
import time
import uuid
import six

from mazepa.queue import Queue
from mazepa.job import Job, AllJobsIndicator


class Scheduler:
    def __init__(
        self,
        queue_name=None,
        completion_queue_name=None,
        queue_region=None,
        threads=16,
        max_task_batch=20000,
        job_status_object=None,
        command="",
        command_name=None,
    ):
        self.queue = Queue(
            queue_name=queue_name,
            completion_queue_name=completion_queue_name,
            queue_region=queue_region,
            threads=threads,
        )

        self._job_status_object = job_status_object

        self.unfinished_jobs = {}
        self.finished_jobs = {}
        self.max_task_batch = max_task_batch

        # when we restart jobs from a checkpoint file, we can only rely on the order the jobs were
        # submitted to determine which job is at which step (because user-specified names for jobs are optional).
        # therefore, we keep track of how many jobs have been submitted so far.
        self._current_job_counter = 0

        # where to store the checkpoint file for the current run.
        home_dir = os.path.expanduser("~")
        mazepa_dir = os.path.join(home_dir, ".mazepa")
        datetime_string = (
            datetime.datetime.now()
            .isoformat(timespec="seconds")
            .replace("-", "_")
            .replace(":", "_")
        )
        if command_name is None:
            checkpoint_filename = (
                f"job_checkpoints/job_checkpoint_{datetime_string}.json"
            )
        else:
            checkpoint_filename = f"job_checkpoints/{command_name}/job_checkpoint_{datetime_string}.json"
        self._job_check_point_filepath = os.path.join(
            mazepa_dir, checkpoint_filename
        )
        os.makedirs(
            os.path.dirname(self._job_check_point_filepath), exist_ok=True
        )

        self._command = command

    def all_jobs_finished(self):
        return bool(self.unfinished_jobs)

    def submit_ready_jobs(self):
        jobs_ready = self.queue.get_completed()
        if jobs_ready is not None:
            self.submit_jobs(jobs_ready)
        else:
            unsubmitted_jobs = self.queue.get_unsubmitted_jobs(
                self.unfinished_jobs
            )
            if unsubmitted_jobs is not None:
                self.submit_jobs(unsubmitted_jobs)

    def execute_until_completion(self, sleep_gap_sec=1):
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

        for job_name, job_object in six.iteritems(self.unfinished_jobs):
            job_object["metadata"]["task_batch_number"] = (
                job_object["metadata"]["task_batch_number"] + 1
            )
            job = job_object["job"]
            this_job_tasks = []
            # if this job is flagged for execution
            if isinstance(jobs_spec, AllJobsIndicator) or job_name in jobs_spec:
                if isinstance(job, Job):
                    this_job_tasks = job.get_task_batch()
                    print(
                        "Got {} tasks from job '{}'".format(
                            len(this_job_tasks), job_name
                        )
                    )
                    if this_job_tasks == []:
                        jobs_just_finished.append(job_name)
                else:
                    try:
                        this_job_tasks = job()
                    except StopIteration:
                        print("Job '{}' is done!")
                        jobs_just_finished.append(job_name)
                for t in this_job_tasks:
                    t.job_name = job_name

                tasks.extend(this_job_tasks)
                if len(tasks) > self.max_task_batch:
                    break

        # Move finished jobs to finished dict
        for job_name in jobs_just_finished:
            print("Flagging job as FINISHED: '{}'".format(job_name))
            self.finished_jobs[job_name] = self.unfinished_jobs[job_name]
            del self.unfinished_jobs[job_name]

        self._write_job_checkpoint_file()

        if len(tasks) > 0:
            print("Scheduling {} tasks..".format(len(tasks)))
            self.queue.submit_mazepa_tasks(tasks)
        else:
            print("No tasks to submit!")

    def register_job(self, job, job_name=None):
        """
        job must be either an istance of type mazepa.Job
        or a simple generator
        """
        if not isinstance(job, Job):
            if not inspect.isgeneratorfunction(job) and not inspect.isgenerator(
                job
            ):
                raise Exception(
                    "Registering job that is \
                               neither Mazepa job nor a generator is\
                               not supported. Submitted job type: {}".format(
                        type(job)
                    )
                )

        if job_name is None:
            job_name = uuid.uuid1()
            while (
                job_name in self.unfinished_jobs
                or job_name in self.finished_jobs
            ):
                job_name = uuid.uuid1()
        else:
            if (
                job_name in self.unfinished_jobs
                or job_name in self.finished_jobs
            ):
                raise Exception(
                    'Unable to register task "{}": \
                        task with name "{}" is already registered'.format(
                        job_name, job_name
                    )
                )

        self._set_job_to_checkpoint(job, job_name)

        return job_name

    def unregister_job(self, job_name):
        if job_name in self.unfinished_jobs:
            del self.unfinished_jobs[job_name]

    def _set_job_to_checkpoint(self, job, job_name):
        if (
            str(self._current_job_counter)
            in self._job_status_object["unfinished_jobs"]
        ):
            task_batches_completed = self._job_status_object["unfinished_jobs"][
                str(self._current_job_counter)
            ]["task_batch_number"]
            self.unfinished_jobs[job_name] = {
                "job": job,
                "metadata": {
                    "job_number": self._current_job_counter,
                    "task_batch_number": task_batches_completed,
                },
            }
            # fast-forward job to batch specified by checkpoint file
            for i in range(task_batches_completed):
                job_finished = False
                if isinstance(job, Job):
                    this_job_tasks = job.get_task_batch()
                    if this_job_tasks == []:
                        job_finished = True
                else:
                    try:
                        this_job_tasks = job()
                    except StopIteration:
                        job_finished = True
                if job_finished:
                    raise Exception(
                        f"Checkpoint file says job {job_name} on task batch {task_batches_completed}, but job only has {i} task batches"
                    )
        elif (
            str(self._current_job_counter)
            in self._job_status_object["finished_jobs"]
        ):
            task_batches_completed = self._job_status_object["finished_jobs"][
                str(self._current_job_counter)
            ]["task_batch_number"]
            self.finished_jobs[job_name] = {
                "job": job,
                "metadata": {
                    "job_number": self._current_job_counter,
                    "task_batch_number": task_batches_completed,
                },
            }
            print(
                "Job '{}' marked as completed in status file".format(job_name)
            )
        else:
            self.unfinished_jobs[job_name] = {
                "job": job,
                "metadata": {
                    "job_number": self._current_job_counter,
                    "task_batch_number": 0,
                },
            }
        # increment counter for next job to register
        self._current_job_counter = self._current_job_counter + 1

    def _write_job_checkpoint_file(self):
        unfinished_jobs_object = {}
        finished_jobs_object = {}
        for job_name, job_object in self.unfinished_jobs.items():
            job_number = job_object["metadata"]["job_number"]
            unfinished_jobs_object[job_number] = {
                "task_batch_number": job_object["metadata"][
                    "task_batch_number"
                ],
                "job_name": job_name,
            }
        for job_name, job_object in self.finished_jobs.items():
            job_number = job_object["metadata"]["job_number"]
            finished_jobs_object[job_number] = {
                "task_batch_number": job_object["metadata"][
                    "task_batch_number"
                ],
                "job_name": job_name,
            }
        job_checkpoint_object = {
            "command": self._command,
            "job_status": {
                "unfinished_jobs": unfinished_jobs_object,
                "finished_jobs": finished_jobs_object,
            },
        }
        with open(self._job_check_point_filepath, "w") as f:
            f.write(json.dumps(job_checkpoint_object, indent=4))
