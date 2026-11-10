#!/usr/bin/env python3
"""Client for the elastic networkscan cloud agent.

  agent.py submit --command "discover port --top-ports 100 --target 128.199.158.128" [--wait]
  agent.py submit --file jobs.json [--wait]
  agent.py status <job_id> [<job_id> ...]
  agent.py results <job_id>
  agent.py collect [--out results/]
  agent.py pools

Job format (public_cloud/region optional, default AWS/us-east-2):
  {"cli": "networkscan", "command": "...", "public_cloud": "AWS", "region": "us-east-2"}
"""
import argparse
import json
import os
import sys
import time
import uuid
from collections import defaultdict
from functools import cache

import boto3

PROFILE = os.environ.get("AWS_PROFILE", "method-takehome")
DEFAULT_LOCATION = ("AWS", "us-east-2")

# Deployed agent pools: (public_cloud, region) -> CloudFormation stack name.
# Each pool is one infra/template.yaml stack. Add a row per Region/cloud you deploy to.
POOLS = {
    ("AWS", "us-east-2"): "cloud-agent",
}


class Pool:
    """Handles to one deployed agent pool, resolved from its stack outputs."""

    def __init__(self, cloud: str, region: str, stack: str):
        if cloud != "AWS":
            raise NotImplementedError(f"{cloud} pools are not implemented yet")
        self.cloud, self.region, self.stack = cloud, region, stack
        session = boto3.Session(profile_name=PROFILE, region_name=region)
        self.sqs, self.ecs = session.client("sqs"), session.client("ecs")
        self.s3, self.logs = session.client("s3"), session.client("logs")
        outputs = session.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
        out = {o["OutputKey"]: o["OutputValue"] for o in outputs}
        self.queue_url = out["JobsQueueUrl"]
        self.bucket = out["ResultsBucketName"]
        self.cluster = out["ClusterName"]

    def result_key(self, job_id: str) -> str:
        return f"results/{job_id}/output.json"


@cache
def get_pool(cloud: str, region: str) -> Pool:
    cloud = cloud.upper()
    stack = POOLS.get((cloud, region))
    if stack is None:
        available = ", ".join(f"{c}/{r}" for c, r in POOLS)
        sys.exit(f"no agent pool deployed for {cloud}/{region} (available: {available})")
    return Pool(cloud, region, stack)


def submit(jobs: list[dict]) -> list[tuple[str, Pool]]:
    by_pool = defaultdict(list)
    for job in jobs:
        cloud = job.get("public_cloud", DEFAULT_LOCATION[0])
        region = job.get("region", DEFAULT_LOCATION[1])
        by_pool[get_pool(cloud, region)].append(job)  # validates every job before sending any

    job_ids = []
    for pool, pool_jobs in by_pool.items():
        for i in range(0, len(pool_jobs), 10):  # SQS batch limit
            entries = []
            for n, job in enumerate(pool_jobs[i : i + 10]):
                job_id = str(uuid.uuid4())  # fresh id: identical commands stay independent
                body = {
                    "job_id": job_id,
                    "cli": job.get("cli", "networkscan"),
                    "command": job["command"],
                    "public_cloud": pool.cloud,
                    "region": pool.region,
                }
                entries.append({"Id": str(n), "MessageBody": json.dumps(body)})
                job_ids.append((job_id, pool))
            resp = pool.sqs.send_message_batch(QueueUrl=pool.queue_url, Entries=entries)
            if resp.get("Failed"):
                sys.exit(f"failed to enqueue: {resp['Failed']}")
    return job_ids


def default_pool() -> Pool:
    return get_pool(*DEFAULT_LOCATION)


def fetch_result(pool: Pool, job_id: str) -> dict | None:
    try:
        obj = pool.s3.get_object(Bucket=pool.bucket, Key=pool.result_key(job_id))
    except pool.s3.exceptions.NoSuchKey:
        return None
    return json.loads(obj["Body"].read())


def job_status(job_id: str, pool: Pool | None = None) -> dict:
    pool = pool or default_pool()
    status = {"job_id": job_id, "pool": f"{pool.cloud}/{pool.region}"}
    for desired in ("RUNNING", "STOPPED"):
        arns = pool.ecs.list_tasks(cluster=pool.cluster, startedBy=job_id, desiredStatus=desired)["taskArns"]
        if arns:
            task = pool.ecs.describe_tasks(cluster=pool.cluster, tasks=arns)["tasks"][0]
            exit_codes = {c["name"]: c.get("exitCode") for c in task["containers"]}
            status.update(status=task["lastStatus"], exit_codes=exit_codes, stopped_reason=task.get("stoppedReason"))
            return status
    # ECS forgets stopped tasks after ~1h; S3 is the durable record.
    if fetch_result(pool, job_id) is not None:
        status["status"] = "STOPPED"
        status["result"] = f"s3://{pool.bucket}/{pool.result_key(job_id)}"
    else:
        status["status"] = "QUEUED_OR_UNKNOWN"
    return status


def open_ports(result: dict) -> dict[str, list[int]]:
    ports = defaultdict(set)
    for sock in (result.get("content", {}).get("result") or {}).get("sockets") or []:
        for p in sock.get("ports") or []:
            ports[sock.get("ip") or sock.get("host")].add(p["port"])
    return {host: sorted(v) for host, v in ports.items()}


def wait(jobs: list[tuple[str, Pool]], poll: int = 5) -> None:
    pending = dict(jobs)
    while pending:
        for job_id, pool in sorted(pending.items()):
            s = job_status(job_id, pool)
            if s["status"] != "STOPPED":
                continue
            # The uploader sidecar is the last container to exit, so the result is in S3 by now.
            del pending[job_id]
            result = fetch_result(pool, job_id)
            summary = open_ports(result) if result else "no output"
            print(f"DONE   {job_id}  exit={s.get('exit_codes')}  open_ports={summary}")
        if pending:
            print(f"...    {len(pending)} job(s) still queued/running")
            time.sleep(poll)


def collect(out_dir: str) -> None:
    """Download every job's output.json to out_dir and print an aggregate view."""
    os.makedirs(out_dir, exist_ok=True)
    aggregate = defaultdict(set)
    jobs = 0
    for (cloud, region) in POOLS:
        pool = get_pool(cloud, region)
        for page in pool.s3.get_paginator("list_objects_v2").paginate(Bucket=pool.bucket, Prefix="results/"):
            for obj in page.get("Contents", []):
                job_id = obj["Key"].split("/")[1]
                path = os.path.join(out_dir, f"{job_id}.json")
                if not os.path.exists(path):
                    pool.s3.download_file(pool.bucket, obj["Key"], path)
                with open(path) as f:
                    for host, ports in open_ports(json.load(f)).items():
                        aggregate[host].update(ports)
                jobs += 1
    print(f"{jobs} result file(s) in {out_dir}/")
    for host, ports in sorted(aggregate.items()):
        print(f"  {host}: open ports {sorted(ports)}")
    with open(os.path.join(out_dir, "aggregate.json"), "w") as f:
        json.dump({h: sorted(p) for h, p in aggregate.items()}, f, indent=2)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("submit")
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--command", help="networkscan arguments, e.g. 'discover port --target x'")
    g.add_argument("--file", help="JSON list of jobs")
    s.add_argument("--cli", default="networkscan")
    s.add_argument("--public-cloud", default=DEFAULT_LOCATION[0])
    s.add_argument("--region", default=DEFAULT_LOCATION[1])
    s.add_argument("--wait", action="store_true", help="wait for all jobs to finish")
    st = sub.add_parser("status")
    st.add_argument("job_ids", nargs="+")
    r = sub.add_parser("results")
    r.add_argument("job_id")
    c = sub.add_parser("collect")
    c.add_argument("--out", default="results")
    sub.add_parser("pools")
    args = p.parse_args()

    if args.cmd == "submit":
        if args.file:
            with open(args.file) as f:
                jobs = json.load(f)
        else:
            jobs = [{"cli": args.cli, "command": args.command, "public_cloud": args.public_cloud, "region": args.region}]
        submitted = submit(jobs)
        for job_id, pool in submitted:
            print(f"QUEUED {job_id}  ({pool.cloud}/{pool.region})")
        if args.wait:
            wait(submitted)
    elif args.cmd == "status":
        for job_id in args.job_ids:
            print(json.dumps(job_status(job_id)))
    elif args.cmd == "results":
        result = fetch_result(default_pool(), args.job_id)
        print(json.dumps(result, indent=2) if result else "no result yet")
    elif args.cmd == "collect":
        collect(args.out)
    elif args.cmd == "pools":
        for (cloud, region), stack in POOLS.items():
            print(f"{cloud}/{region} -> stack {stack}")


if __name__ == "__main__":
    main()
