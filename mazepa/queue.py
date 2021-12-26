import boto3
import tenacity
import taskqueue
import time
import uuid
import multiprocessing
import functools
import json
from collections import defaultdict

from taskqueue import RegisteredTask as TQRegisteredTask

import mazepa

retry = tenacity.retry(
  reraise=True,
  stop=tenacity.stop_after_attempt(4),
  wait=tenacity.wait_full_jitter(0.5, 60.0),
  )


# TQ wrapper. Theoretically don't have to use TQ library, but it's nice
class MazepaTaskTQ(TQRegisteredTask):
    def __init__(self, task=None,
            task_spec=None,
            completion_queue_name=None,
            completion_queue_region=None,
            task_id=None,
            job_id=None
            ):

        if task is not None:
            task_spec = mazepa.serialize(task)
            task_id = str(uuid.uuid4())
            job_id = task.job_name

        super().__init__(task_spec=task_spec,
                task_id=task_id, job_id=job_id,
                completion_queue_name=completion_queue_name,
                completion_queue_region=completion_queue_region)

    def execute(self):
        task = mazepa.deserialize(self.task_spec)
        print (f"Starting execution of {type(task)}")
        ts = time.time()
        task.execute()
        te = time.time()
        print (f"Done! Execution time: {te - ts} seconds. ")
        if self.completion_queue_name is not None:
            if self.job_id is None:
                raise Exception("'job_id' property must be set "
                        "for each task")
            send_completion_report(
                    queue_name=self.completion_queue_name,
                    queue_region=self.completion_queue_region,
                    task_id=self.task_id,
                    job_id=self.job_id
                    )

def send_completion_report(queue_name, queue_region,
        task_id, job_id):
    completion_queue = taskqueue.TaskQueue(
                                 queue_name,
                                 region_name=queue_region,
                                 n_threads=0,
                                 green=False)
    message = [{
        'Id': 'completion_report',
        'MessageBody':json.dumps({
                "task_id": task_id,
                "job_id": job_id
        })
    }]
    send_message(completion_queue, message)


def send_message(q, message):
    msg_ack = q.api.sqs.send_message_batch(
            QueueUrl=q.api.qurl,
            Entries=message
        )

    success = False
    for i in range(20):
        if 'Successful' in msg_ack and \
            len(msg_ack['Successful']) > 0:
            success = True
            break
        else:
            msg_ack = q.api.sqs.send_message_batch(
                    QueueUrl=q.api.qurl,
                    Entries=message
                )

    if success == False:
        raise ValueError(f"Failed to send message {message} to "
                         f"queue {q.api.qurl}")


class Queue:
    def __init__(self, queue_name=None, completion_queue_name=None,
            threads=0, queue_region=None):
        self.threads = threads
        self.queue_name = queue_name
        self.queue_region = queue_region
        self.completion_queue_name = completion_queue_name
        self.completion_registry = None
        self.constructor_pool = multiprocessing.Pool(self.threads)

        if queue_name is None:
            self.local_execution = True
        else:
            self.local_execution = False
            if queue_name.startswith('fq://'):
                self.queue_type = 'fq'
                self.queue = taskqueue.TaskQueue(queue_name, green=False)
            else:
                self.queue_type = 'sqs'
                self.queue = taskqueue.TaskQueue(queue_name,
                        region=queue_region, green=False)
                self.queue_boto = boto3.client('sqs',
                                               region_name=queue_region)
                self.queue_url = self.queue_boto.get_queue_url(
                        QueueName=self.queue_name)["QueueUrl"]

            if completion_queue_name is not None:
                if self.queue_type == 'fq':
                    raise NotImplementedError("Completion queue is not supported with file queue")

                self.completion_registry = None
                self.completion_queue = \
                        taskqueue.TaskQueue(
                                completion_queue_name,
                                region=queue_region,
                                green=False)
                self.completion_queue_url = \
                        self.queue_boto.get_queue_url(
                              QueueName=
                                  completion_queue_name)["QueueUrl"]

                self.completion_registry = defaultdict(lambda: {})

            else:
                self.completion_queue = None
                self.completion_queue_url = None

    def submit_tq_tasks(self, tq_tasks):
        if self.completion_queue is not None:
            for t in tq_tasks:
                self.completion_registry[t.job_id][t.task_id] = True

        self.queue.insert(tq_tasks, parallel=self.threads)

    def submit_mazepa_tasks(self, mazepa_tasks):
        if self.local_execution:
            print ("Starting local task execution...")
            for t in mazepa_tasks:
                t.execute()
        else:
            task_constructor = functools.partial(
                MazepaTaskTQ,
                completion_queue_name=self.completion_queue_name,
                completion_queue_region=self.queue_region
            )

            print ("Converting tasks...")
            if len(mazepa_tasks) > 10000:
                tq_tasks = self.constructor_pool.map(task_constructor, mazepa_tasks)
            else:
                tq_tasks = [task_constructor(mt) for mt in mazepa_tasks]
            print ("Submitting...")
            self.submit_tq_tasks(tq_tasks)


    @retry
    def remote_queue_is_empty(self):
        """Is our remote queue empty?
        """
        if self.queue_type == 'sqs':
            attribute_names = ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            for i in range(10):
                response = self.queue_boto.get_queue_attributes(
                        QueueUrl=self.queue_url,
                        AttributeNames=attribute_names)
                for a in attribute_names:
                    num_messages = int(response['Attributes'][a])
                    if num_messages > 0:
                        return False
                if i < 9:
                    time.sleep(0.5)

            return True
        elif self.queue_type == 'fq':
            return self.queue.is_empty()
        else:
            raise Exception(f"Unsupported queue type: '{self.queue_type}'")

    def is_local_queue(self):
        return self.local_execution

    def is_remote_queue(self):
        return not self.is_local_queue()

    def get_completed(self):
        if self.is_local_queue():
            return mazepa.job.AllJobsIndicator()
        else:
            if self.completion_queue is None:
                if self.remote_queue_is_empty():
                    return mazepa.job.AllJobsIndicator()
                else:
                    time.sleep(5)
                    return None
            else:
                completed_jobs = self.check_completion_report()
                if completed_jobs is not None:
                    return completed_jobs
                else:
                    if self.remote_queue_is_empty():
                        return mazepa.job.AllJobsIndicator()
                    else:
                        time.sleep(1)
                        return None


    def check_completion_report(self):
        print ("Checking completion report...")
        completed_jobs = []
        while True:
            resp = self.queue_boto.receive_message(
                            QueueUrl=self.completion_queue_url,
                            AttributeNames=['All'],
                            MaxNumberOfMessages=10)

            if 'Messages' not in resp:
                print ("No more messages!")
                break
            else:
                entries = []
                completed_tasks = []

                for i in range(len(resp['Messages'])):
                    entries.append({
                        'ReceiptHandle':\
                            resp['Messages'][i]['ReceiptHandle'],
                        'Id': str(i)
                    })
                    completed_tasks.append(json.loads(
                            resp['Messages'][i]['Body']))

                self.queue_boto.delete_message_batch(
                    QueueUrl=self.completion_queue_url,
                    Entries=entries
                )
            for t in completed_tasks:
                if t['job_id'] in self.completion_registry and \
                        t['task_id'] in self.completion_registry[t['job_id']]:
                    print (f"Deleting task {t['task_id']} from job {t['job_id']}")
                    del self.completion_registry[t['job_id']][t['task_id']]

                    if len(self.completion_registry[t['job_id']]) == 0:
                        completed_jobs.append(t['job_id'])
                        del self.completion_registry[t['job_id']]
                else:
                    print (f"Unregistered task {t['task_id']} from job {t['job_id']}")

            if len(completed_jobs) > 5:
                print (f"Have some completed jobs, stoping polling")
                break

        print (f"Returning {len(completed_jobs)} completed jobs")
        if len(completed_jobs) > 0:
            return completed_jobs
        else:
            return None

    def get_unsubmitted_jobs(self, full_job_list):
        unsubmitted_jobs = []
        if self.completion_queue is not None:
            for j in full_job_list:
                if j not in self.completion_registry:
                    unsubmitted_jobs.append(j)
        return unsubmitted_jobs

    def poll_tasks(self, lease_seconds):
        assert self.is_remote_queue
        self.queue.poll(lease_seconds=lease_seconds)
