import time
import uuid
import six

from mazepa.queue import Queue
from mazepa.job import Job, AllJobsIndicator

class Executor:
    def __init__(self, queue_name=None):
        self.queue = Queue(queue_name=queue_name)

    def execute(self, lease_seconds):
        self.queue.poll_tasks(lease_seconds=lease_seconds)
