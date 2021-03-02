import boto3
import click

from mazepa.scheduler import Scheduler
from mazepa.executor import Executor


def click_options(cls):
    queue = click.option(
        "--queue_name",
        "-q",
        nargs=1,
        type=str,
        default=None,
        help="Name of the queue where the tasks will be pushed to. "
        "For file queue, use 'fq://{path_to_shared_storage}' format."
        "If no 'fq://' prefix is given, the queue is assumed to be SQS. "
        "If not specified, tasks will be executed locally.",
    )

    completion_queue = click.option(
        "--completion_queue_name",
        nargs=1,
        type=str,
        default=None,
        help="Name of AWS SQS queue where completion of tasks will "
        "be reported. Must be distinct from the 'queue_name'. "
        "Providing a completion queue will speed up execution when "
        "running workers on unreliable machines (preemptible, spot).",
    )

    region = click.option(
        "--sqs_queue_region",
        nargs=1,
        type=str,
        default="us-east-1",
        help="AWS region of  SQS queues. Task queue and completion "
        "queue must share the same region.",
    )

    restart_from_checkpoint_file = click.option(
        "--restart_from_checkpoint_file",
        nargs=1,
        type=str,
        default=None,
        help="Option to be used in case of a job interruption. Specify "
        "path to filename where job progress is logged so that execution "
        "can start from where it left off before the interruption.",
    )

    return queue(completion_queue(region(restart_from_checkpoint_file(cls))))


def parse_queue_params(args):
    queue_name = args["queue_name"]
    completion_queue_name = args["completion_queue_name"]
    queue_region = args["sqs_queue_region"]

    if queue_name is not None:
        s = boto3.Session()
        sqs_regions = s.get_available_regions("sqs")
        if queue_region not in sqs_regions:
            raise Exception(
                f"Invalid AWS SQS region '{queue_region}'. "
                f"Valid AWS SQS region list: {sqs_regions}"
            )

            if queue_name is None and completion_queue_name is not None:
                raise Exception(
                    "'completion_queue_name' can only be "
                    "used when 'queue_name' is provided"
                )
    return {
        "queue_name": queue_name,
        "completion_queue_name": completion_queue_name,
        "queue_region": queue_region,
    }


def validate_jobs_dict(status_object, dict_name):
    status_dict = status_object[dict_name]
    if isinstance(status_dict, dict):
        for job_number, job_info in status_dict.items():
            if "task_batch_number" not in job_info:
                raise Exception(
                    f"Job {job_info} in checkpoint file must specify 'task_batch_number'"
                )
    else:
        raise Exception(
            f"{dict_name} entry in checkpoint file must be a dictionary"
        )


def parse_checkpoint_file(args):
    checkpoint_file_path = args["restart_from_checkpoint_file"]
    if checkpoint_file_path is None:
        return {
            "command": "",
            "job_status": {"unfinished_jobs": {}, "finished_jobs": {}},
        }
    else:
        import json

        with open(checkpoint_file_path) as f:
            try:
                status_object = json.load(f)
            except:
                raise Exception("Checkpoint file is not proper json")
            if "job_status" not in status_object:
                raise Exception("Checkpoint file missing 'job_status' object")
            if ("unfinished_jobs" not in status_object["job_status"]) or (
                "finished_jobs" not in status_object["job_status"]
            ):
                raise Exception(
                    "Checkpoint file must contain 'unfinished_jobs' and 'finished_jobs' dicts"
                )
            validate_jobs_dict(status_object["job_status"], "unfinished_jobs")
            validate_jobs_dict(status_object["job_status"], "finished_jobs")
            return status_object


def parse_scheduler_from_kwargs(args):
    scheduler_params = parse_queue_params(args)
    checkpoint_params = parse_checkpoint_file(args)
    scheduler_params["job_status_object"] = checkpoint_params["job_status"]
    scheduler_params["command"] = checkpoint_params.get("command", "")
    scheduler_params["command_name"] = args["command_name"]
    import sys

    # If running from the command line and not restarting from a file, store the command
    if len(sys.argv) > 1 and (
        "command" not in scheduler_params or scheduler_params["command"] == ""
    ):
        scheduler_params["command"] = " ".join(sys.argv)
    return Scheduler(**scheduler_params)


def parse_executor_from_kwargs(args):
    queue_params = parse_queue_params(args)
    return Executor(**queue_params)
