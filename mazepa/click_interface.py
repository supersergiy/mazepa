import boto3
import click

from mazepa.scheduler import Scheduler
from mazepa.executor import Executor

def click_options(cls):
    queue = click.option('--queue_name', '-q', nargs=1,
            type=str, default=None,
            help="Name of the queue where the tasks will be pushed to. "
            "For file queue, use 'fq://{path_to_shared_storage}' format."
            "If no 'fq://' prefix is given, the queue is assumed to be SQS. "
            "If not specified, tasks will be executed locally.")

    completion_queue = click.option('--completion_queue_name', nargs=1,
            type=str, default=None,
            help="Name of AWS SQS queue where completion of tasks will "
            "be reported. Must be distinct from the 'queue_name'. "
            "Providing a completion queue will speed up execution when "
            "running workers on unreliable machines (preemptible, spot).")

    region = click.option('--sqs_queue_region',  nargs=1,
            type=str, default='us-east-1',
            help="AWS region of  SQS queues. Task queue and completion "
            "queue must share the same region.")

    return queue(completion_queue(region(cls)))

def parse_queue_params(args):
    queue_name = args['queue_name']
    completion_queue_name = args['completion_queue_name']
    queue_region = args['sqs_queue_region']

    if queue_name is not None:
        s = boto3.Session()
        sqs_regions = s.get_available_regions('sqs')
        if queue_region not in sqs_regions:
            raise Exception(f"Invalid AWS SQS region '{queue_region}'. "
                f"Valid AWS SQS region list: {sqs_regions}")

            if queue_name is None and completion_queue_name is not None:
                raise Exception("'completion_queue_name' can only be "
                        "used when 'queue_name' is provided")
    return {
            'queue_name': queue_name,
            'completion_queue_name': completion_queue_name,
            'queue_region': queue_region
        }

def parse_scheduler_from_kwargs(args):
    queue_params = parse_queue_params(args)
    return Scheduler(**queue_params)

def parse_executor_from_kwargs(args):
    queue_params = parse_queue_params(args)
    return Executor(**queue_params)
