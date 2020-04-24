import taskqueue
import tenacity
import boto3
import .job_ready

retry = tenacity.retry(
  reraise=True,
  stop=tenacity.stop_after_attempt(4),
  wait=tenacity.wait_full_jitter(0.5, 60.0),

class BasicQueue:
    def submit(self):
        raise NotImplementedError

    def get_completed(self):
        raise NotImplementedError

class Queue(BasicQueue):
    def __init__(self, queue_name=None, threads=1, queue_region='us-east-1'):
        self.threads = threads
        self.queue_name = queue_name
        if queue_name is None:
            self.queue = taskqueue.GreenTaskQueue(queue_name)
            self.queue_boto = boto3.client('sqs',
                                            region_name=self.queue_region)
            self.queue_url = self.queue_boto.get_queue_url(self.queue_name)["QueueUrl"]
        else:
            #TODO: more lightweight?
            self.queue = taskqueue.LocalTaskQueue(parallel=1)

    def submit(self, tasks):
        if self.threads > 1:
            #TODO
            raise NotImplementedError
        self.queue.insert_all(tasks)

    @retry
    def remote_queue_is_empty(self):
        """Is our remote queue empty?
        """
        #TODO: cleanup
        attribute_names = ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        responses = []
        for i in range(3):
            response = self.queue_boto.get_queue_attributes(QueueUrl=self.queue_url,
                                                     AttributeNames=attribute_names)
            for a in attribute_names:
                responses.append(int(response['Attributes'][a]))
                print('{}     '.format(responses[-2:]), end="\r", flush=True)
            if i < 2:
              sleep(1)

        return all(n == 0 for n in responses)

    def is_local_queue(self):
        return isinstance(self.queue, taskqueue.LocalTaskQueue)

    def is_remote_queue(self):
        return not self.is_local_queue()

    def get_completed(self):
        if self.is_local_queue() or self.remote_queue_is_emtpy()
            return job_ready.AllJobsReady()
        else:
            return None




