import boto3
import tenacity
import taskqueue
import time
import uuid
from collections import defaultdict

from taskqueue import RegisteredTask as TQRegisteredTask

import mazepa

retry = tenacity.retry(
  reraise=True,
  stop=tenacity.stop_after_attempt(4),
  wait=tenacity.wait_full_jitter(0.5, 60.0),
  )


# TQ wrapper. Theretically don't have to use TQ library, but it's nice
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
    completion_queue = TaskQueue(queue_name=completed_queue_name,
                                 region_name=queue_region,
                                 n_threads=0)

    queue_api_obj = completion_queue._api
    queue_sqs_obj = completion_queue._api._sqs
    message = [{
        'Id': 'Completion Report',
        'MessageBody':json.dumps({
                "task_id": task_id,
                "job_id": job_id
        })
    }]
    send_message(queue_api_obj, queue_sqs_obj, message)


def send_message(queue_api_obj, queue_sqs_obj, message):
    msg_ack = queue_sqs_obj.send_message_batch(
            QueueUrl=queue_api_obj._qurl,
            Entries=message
        )

    success = False
    for i in range(20):
        if 'Successful' in msg_ack and \
            len(msg_ack['Successful']) > 0:
            success = True
            break
        else:
            msg_ack = queue_sqs_obj.send_message_batch(
                    QueueUrl=queue_api_obj._qurl,
                    Entries=message
                )

    if success == False:
        raise ValueError(f"Failed to send message {message} to "
                         f"queue {queue_api_obj._qurl}")


class Queue:
    def __init__(self, queue_name=None, completion_queue_name=None,
            threads=1, queue_region=None):
        self.threads = threads
        self.queue_name = queue_name
        self.completion_queue_name = completion_queue_name
        self.completion_registry = None

        if queue_name is None:
            self.local_execution = True
            #self.queue = taskqueue.LocalTaskQueue(parallel=1)
        else:
            self.local_execution = False
            self.queue = taskqueue.GreenTaskQueue(queue_name,
                    region=queue_region)
            self.queue_boto = boto3.client('sqs',
                                            region_name=queue_region)
            self.queue_url = self.queue_boto.get_queue_url(
                    QueueName=self.queue_name)["QueueUrl"]

            if completion_queue_name is not None:
                self.completion_registry = None
                self.completion_queue = \
                        taskqueue.GreenTaskQueue(
                                completion_queue_name,
                                region=queue_region)
                self.completion_queue_url = \
                        self.queue_boto.get_queue_url(
                              QueueName=
                                  completion_queue_name)["QueueUrl"]

                self.completion_registry = defaultdict(lambda: [])

            else:
                self.completion_queue = None
                self.completion_queue_url = None

    def submit_tq_tasks(self, tq_tasks):
        if self.threads > 1:
            #TODO
            raise NotImplementedError
        if self.completion_queue is not None:
            for t in tq_tasks:
                self.completion_registry[t.job_id].append(t.task_id)

        self.queue.insert_all(tq_tasks)

    def submit_mazepa_tasks(self, mazepa_tasks):
        if self.threads > 1:
            #TODO
            raise NotImplementedError
        if self.local_execution:
            print ("Starting local task execution...")
            for t in mazepa_tasks:
                t.execute()
        else:
            tq_tasks = [MazepaTaskTQ(t) for t in mazepa_tasks]
            self.submit_tq_tasks(tq_tasks)

    @retry
    def remote_queue_is_empty(self):
        """Is our remote queue empty?
        """
        #TODO: cleanup
        attribute_names = ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        responses = []
        for i in range(3):
            response = self.queue_boto.get_queue_attributes(
                    QueueUrl=self.queue_url,
                    AttributeNames=attribute_names)
            for a in attribute_names:
                responses.append(int(response['Attributes'][a]))
                print('{}     '.format(responses[-2:]),
                                           end="\r", flush=True)
            if i < 2:
              time.sleep(1)

        return all(n == 0 for n in responses)

    def is_local_queue(self):
        return self.local_execution

    def is_remote_queue(self):
        return not self.is_local_queue()

    def get_completed(self):
        if self.is_local_queue():
            return mazepa.job.AllJobsIndicator()
        else:
            if self.remote_queue_is_empty():
                return mazepa.job.AllJobsIndicator()
            elif self.completion_queue is not None:
                return self.check_completion_report()
            else:
                return None

    def check_completion_report(self):
        while True:
            resp = self.completion_queue_boto.receive_message(
                            QueueUrl=self.completion_queue_url,
                            AttributeNames=['All'],
                            MaxNumberOfMessages=10)

            if 'Messages' not in resp:
                return None
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

                self.completion_queue_boto.delete_message_batch(
                        QueueUrl=self.completion_queue_url,
                        Entries=entries)

            completed_jobs = []
            for t in completed_tasks:
                del self.completion_registry[t.job_id][t.task_id]
                if len(self.completion_registry[t.job_id]) == 0:
                    completed_jobs.append(t.job_id)

            if len(completed_jobs) > 0:
                return completed_jobs

    def poll_tasks(self, lease_seconds):
        assert self.is_remote_queue
        self.queue.poll(lease_seconds=lease_seconds)
