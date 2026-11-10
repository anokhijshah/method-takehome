"""SQS-triggered dispatcher: launches one Fargate task per networkscan job.

Message body: {"job_id": "...", "cli": "networkscan", "command": "discover port ...",
               "public_cloud": "AWS", "region": "us-east-2"}
"""
import json
import logging
import os
import re
import shlex

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ecs = boto3.client("ecs")

CLUSTER = os.environ["CLUSTER"]
TASK_DEFINITION = os.environ["TASK_DEFINITION"]
SCAN_CONTAINER = os.environ.get("SCAN_CONTAINER", "networkscan")
UPLOAD_CONTAINER = os.environ.get("UPLOAD_CONTAINER", "uploader")
SUBNETS = os.environ["SUBNETS"].split(",")
SECURITY_GROUP = os.environ["SECURITY_GROUP"]
# Shared volume path the uploader sidecar reads from (see infra/template.yaml).
OUTPUT_FILE = "/mnt/output/output.json"

SUPPORTED_CLIS = {"networkscan"}
# Output location and format are owned by the agent so results can always be collected.
RESERVED_FLAGS = {"--output-file", "-f", "--output", "-o"}
JOB_ID_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")  # used in S3 keys and as clientToken


class InvalidJob(ValueError):
    """Job can never succeed; retrying is pointless."""


def strip_flags(args: list[str], flags: set[str]) -> list[str]:
    out, skip = [], False
    for arg in args:
        if skip:
            skip = False
        elif arg in flags:
            skip = True  # drop the flag and its value
        elif arg.split("=", 1)[0] in flags:
            pass  # --flag=value form
        else:
            out.append(arg)
    return out


def parse_job(body: str) -> tuple[str, list[str]]:
    try:
        job = json.loads(body)
        job_id, cli, command = job["job_id"], job["cli"], job["command"]
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        raise InvalidJob(f"malformed job: {e}") from e

    if not isinstance(job_id, str) or not JOB_ID_RE.match(job_id):
        raise InvalidJob(f"invalid job_id: {job_id!r}")
    if cli not in SUPPORTED_CLIS:
        raise InvalidJob(f"unsupported cli: {cli!r}")
    try:
        args = shlex.split(command)
    except ValueError as e:
        raise InvalidJob(f"unparseable command: {e}") from e
    if args and args[0] == cli:
        args = args[1:]
    args = strip_flags(args, RESERVED_FLAGS)
    if not args:
        raise InvalidJob("empty command")
    return job_id, args + ["--output", "json", "--output-file", OUTPUT_FILE]


def launch(job_id: str, args: list[str]) -> str:
    resp = ecs.run_task(
        cluster=CLUSTER,
        taskDefinition=TASK_DEFINITION,
        launchType="FARGATE",
        count=1,
        startedBy=job_id,
        clientToken=job_id,  # idempotent if SQS redelivers the message
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": SUBNETS,
                "securityGroups": [SECURITY_GROUP],
                "assignPublicIp": "ENABLED",
            }
        },
        overrides={
            "containerOverrides": [
                {"name": SCAN_CONTAINER, "command": args},
                {"name": UPLOAD_CONTAINER, "environment": [{"name": "JOB_ID", "value": job_id}]},
            ]
        },
    )
    if resp.get("failures") or not resp.get("tasks"):
        raise RuntimeError(f"RunTask failed: {resp.get('failures')}")
    return resp["tasks"][0]["taskArn"]


def handler(event, context):
    failures = []
    for record in event["Records"]:
        message_id = record["messageId"]
        try:
            job_id, args = parse_job(record["body"])
            task_arn = launch(job_id, args)
            logger.info(json.dumps({"event": "STARTED", "job_id": job_id, "task_arn": task_arn, "args": args}))
        except InvalidJob as e:
            logger.error(json.dumps({"event": "REJECTED", "message_id": message_id, "error": str(e)}))
        except Exception as e:  # throttling, quota, capacity: let SQS retry
            logger.warning(json.dumps({"event": "RETRY", "message_id": message_id, "error": str(e)}))
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
