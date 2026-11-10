# Elastic Cloud Agent for `networkscan`

A prototype system that accepts `networkscan` commands, runs each one on its own short-lived
cloud agent, and collects the results. It scales out when a burst of commands arrives and
scales back to zero when there's nothing to do.

```
                       ┌────────────── AWS (us-east-2), one CloudFormation stack ───────────────┐
 agent.py  ──submit──▶ │ SQS jobs queue ──▶ Lambda dispatcher ──RunTask──▶ Fargate task per job │
 (CLI)                 │      │ 10 failed launches                       ┌──────────────────────┐ │
                       │      ▼                                          │ networkscan container │ │
                       │  dead-letter queue ──▶ CloudWatch alarm         │   │ output.json        │ │
                       │                                                 │   ▼ (shared volume)  │ │
                       │                                                 │ uploader sidecar ─────┼─┼──▶ S3 results/<job_id>/output.json
                       │                                                 └──────────────────────┘ │
                       │  ECS "task stopped" event ──▶ EventBridge ──▶ completion log            │
                       └────────────────────────────────────────────────────────────────────────┘
 agent.py collect  ◀──────────────────────────── S3 ───────────────── results/ + aggregate.json
```

## Quick start

Prerequisites: AWS CLI v2 with credentials for an account, Python 3.10+.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./infra/deploy.sh                      # creates stack "cloud-agent" in the default VPC (~3 min)

.venv/bin/python agent.py submit --command "discover port --top-ports 100 --target 128.199.158.128" --wait
.venv/bin/python agent.py submit --file examples/burst.json --wait     # 7 parallel jobs
.venv/bin/python agent.py collect                                      # download all results + aggregate
.venv/bin/python agent.py status <job_id>
.venv/bin/python -m unittest discover tests                            # unit tests, no AWS needed

./infra/teardown.sh                    # delete everything, including stored results
```

`AWS_PROFILE` / `AWS_REGION` select the account and Region (defaults: `method-takehome`, `us-east-2`).

### Input

Exactly the spec's format, plus optional location fields:

```json
{"cli": "networkscan", "command": "discover port --top-ports 100 --target 128.199.158.128",
 "public_cloud": "AWS", "region": "us-east-2"}
```

Notes:
- The current CLI syntax is `discover port --top-ports`; the spec's `port scan --topports` is from an
  older release. The image is pinned to `methodsecurity/networkscan:0.21.0`.
- The agent owns `--output`/`--output-file` (always JSON, always written to the collection path), so
  any user-supplied values for those flags are replaced.
- Every submission gets a fresh UUID, so identical commands sent twice run twice, as the spec requires.

### Output

- **Completion:** `submit --wait` prints `DONE <job_id> exit={...} open_ports={...}`. Every task stop
  is also recorded in the CloudWatch log group `/aws/events/cloud-agent-jobs` (job id, per-container
  exit codes, timestamps) by an EventBridge rule.
- **Results:** `s3://<ResultsBucket>/results/<job_id>/output.json`. `agent.py collect` downloads all
  of them to `results/` and writes `results/aggregate.json` (host → union of open ports).

## Design decisions

### Why one Fargate task per job

| Option | Why not (for this workload) |
|---|---|
| Long-running EC2 / ECS service workers | Costs money while idle; bursts need warm-up and capacity planning. |
| Lambda running the scan | 15-minute limit, no raw sockets (SYN scan), no control of the binary's environment. |
| **Fargate `RunTask` per job** ✅ | Starts on demand, billed per second, isolated microVM per job, no servers. SYN scanning works (`CAP_NET_RAW` is available). |

Every job is independent, so a job is the natural unit of scaling: N jobs → N agents.

### Why SQS + Lambda in front

- **Async intake:** `submit` returns as soon as the job is enqueued. Bursts are absorbed by the queue.
- **Elastic dispatch:** the SQS → Lambda event source mapping scales dispatchers automatically
  (capped at 5 concurrent, about 50 launches/s, which is more than the Fargate quota can absorb).
- **Retries with backpressure:** if `RunTask` fails for a transient reason (throttling, vCPU quota,
  AZ capacity), only that message is returned (`ReportBatchItemFailures`) and SQS retries it 3
  minutes later, up to 10 times (about 30 minutes), before dead-lettering. A burst larger than the
  quota therefore drains as capacity frees up instead of failing.
- **Poison jobs aren't retried:** malformed input (bad JSON, unknown CLI, unparseable command,
  unsafe job id) is logged as `REJECTED` and dropped.
- **Idempotency:** the job id is passed as `RunTask`'s `clientToken`, so an SQS redelivery can't
  launch the same job twice.

### Speed / cost / reliability

| Objective | How | Measured |
|---|---|---|
| Speed | All jobs start in parallel; the burst finishes in about one job's time | 1 scan ≈ 26 s task runtime; 7 scans ≈ 60–80 s end-to-end (including image pulls and 5 s polling) |
| Cost | Nothing runs between bursts. Idle cost is ≈ $0 (queue, Lambda, logs, S3). | ≈ $0.0001 per top-100 scan (0.25 vCPU / 0.5 GB for ~30 s) |
| Reliability | SQS retries + DLQ + alarm; per-job isolation (one crash can't affect others); results persisted to S3; failures visible as exit codes | Failed scan → `exit={'networkscan': 1, 'uploader': 2}`, task stops cleanly |

### Result collection without modifying Method's image

Each task has two containers sharing an ephemeral volume. `networkscan` writes
`/mnt/output/output.json`; the `uploader` sidecar (official AWS CLI image) starts when the scanner
exits (`dependsOn: COMPLETE`) and copies the file to S3. This avoids building and hosting a custom
image. Only the uploader's IAM role can write, and only to `results/*`.

### Security

- Scan tasks have a security group with **no inbound rules** and get no AWS permissions apart from
  the uploader's S3 write.
- Every role is least-privilege: the dispatcher can run only this task family on this cluster and can
  pass only the two task roles, only to ECS. Trust policies use `aws:SourceAccount`.
- S3 and SQS are encrypted at rest and deny non-TLS access; the bucket blocks all public access.
- Commands are split with `shlex` and passed to the container as an argument list. No shell
  interprets user input. Job ids are restricted to `[A-Za-z0-9-]`.

## Location / metadata (extra)

**Implemented:** jobs carry `public_cloud` and `region`. `agent.py` routes each job to the agent pool
(stack) for that location via the `POOLS` table, and rejects locations with no pool:

```
$ agent.py submit --command "..." --public-cloud AZURE --region eastus
no agent pool deployed for AZURE/eastus (available: AWS/us-east-2)
```

**Adding a location:**
- **Another AWS Region:** `AWS_REGION=eu-west-1 ./infra/deploy.sh` and add one row to `POOLS`. The
  template is Region-agnostic. (My test account can only use us-east-2, so this wasn't exercised.)
- **Azure:** the same shape maps onto Azure Storage Queue → Azure Function → Container Apps Jobs
  (or Azure Container Instances) → Blob Storage. The client already has the routing hook:
  `Pool` would get an Azure implementation behind the same `submit`/`status`/`fetch_result` interface.
- **A central router in the cloud:** for production I'd move routing server-side (API Gateway → a
  router that forwards to the pool's queue), so clients send to one endpoint.

**Controlling the source IP (the spec's use cases):**
- **Random IP (default):** every Fargate task gets a fresh public IP from AWS's pool, so the scan
  source changes between jobs.
- **Static IP:** run tasks in private subnets behind a NAT gateway with an Elastic IP. All scans then
  come from one known address that defenders can allow-list. This costs about $32/month per NAT
  gateway while it exists, so I'd make it a per-pool option (`EgressMode: static|random`) and select
  it per job with an `egress` field.
- **Country / cloud:** choose the pool by Region, as above.

## Limitations and next steps

- **Quota:** the account's Fargate quota is 12 vCPU, so at most 48 concurrent 0.25-vCPU scans. Jobs
  beyond that wait in the queue (and dead-letter after about 30 minutes). Raise the quota for real use.
- **Big scans aren't split:** a /16 sweep or full 65,535-port scan runs as one job. The next step
  is a fan-out stage that splits targets (CIDR blocks) or port ranges into many jobs and merges results.
- **Single task size:** 0.25 vCPU / 0.5 GB for every job; heavier commands would need per-job sizing
  (`RunTask` CPU/memory overrides).
- **Status lookups:** `status` reads live state from ECS, which forgets stopped tasks after about an
  hour; after that it falls back to S3. A small DynamoDB job table updated from the EventBridge
  rule would give a complete job history (queued/running/succeeded/failed).
- **Image pull time** dominates short scans. Copying the image into ECR in-Region, or using
  SOCI lazy loading, would cut start-up time.
- **No auth on submission:** the client uses the caller's AWS credentials directly. A hosted API
  would need authentication and per-tenant queues.

## Repository layout

```
agent.py                 client: submit / status / results / collect / pools
dispatcher/handler.py    Lambda: SQS batch → validate → ecs:RunTask (one task per job)
infra/template.yaml      CloudFormation: queue, DLQ, Lambda, ECS, S3, IAM, EventBridge, alarms
infra/deploy.sh          package + validate + deploy (auto-detects default VPC subnets)
infra/teardown.sh        delete stack and buckets
tests/test_handler.py    unit tests for parsing, retries, idempotency
examples/burst.json      7-job burst (6 identical + 1 different)
```
