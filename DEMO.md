# Demo run sheet (≈10 min of the 30)

**Before the call:** run `aws login --profile method-takehome` (sessions expire). Then open these
tabs in the us-east-2 console:
1. ECS → Clusters → `cloud-agent` → Tasks (filter: all statuses)
2. SQS → `cloud-agent-jobs` (Monitoring tab)
3. CloudWatch → Log groups → `/aws/events/cloud-agent-jobs`
4. S3 → `cloud-agent-resultsbucket-…` → `results/`

Do one warm-up run before the call so the commands are fresh in your terminal history.

## 1. Architecture (2 min)
Show the README diagram. Key line: *"Each command becomes one Fargate task. The queue absorbs
bursts, and nothing runs (or costs money) between bursts."*

## 2. Single job (2 min)
```bash
.venv/bin/python agent.py submit --command "discover port --top-ports 100 --target 128.199.158.128" --wait
```
While it runs, switch to the ECS tab and refresh: one task appears (PROVISIONING → RUNNING →
STOPPED). Point out the two containers: `networkscan` + `uploader`.

## 3. Burst / elasticity (3 min)
```bash
.venv/bin/python agent.py submit --file examples/burst.json --wait
```
- ECS tab: 7 tasks running at once.
- Six identical commands run six times, because commands are independent (spec).
- The total time is about the same as one job.

## 4. Results + completion log (2 min)
```bash
.venv/bin/python agent.py collect
cat results/aggregate.json
```
Show the S3 tab (`results/<job_id>/output.json`) and the completion log group (one event per job,
with exit codes).

## 5. Failure handling (1 min)
```bash
.venv/bin/python agent.py submit --command "discover nonexistent --target 128.199.158.128" --wait
.venv/bin/python agent.py submit --command "discover port --target 1.2.3.4" --public-cloud AZURE --region eastus
```
- Tool failure → `exit={'networkscan': 1, 'uploader': 2}`; the task still stops cleanly.
- Unknown location → rejected client-side, listing the available pools.
- Mention: transient launch failures are retried by SQS (10 tries, then the DLQ + alarm).

## Likely questions (short answers)
- **Why not Lambda for the scan?** 15-minute limit, no raw sockets for SYN scans.
- **Why not a warm worker pool?** Idle cost. Trade-off: about 20–40 s cold start per job. A warm
  pool or in-Region ECR image + SOCI would cut that.
- **How does it scale further?** Raise the Fargate vCPU quota (12 now = 48 scans); split large
  targets into many jobs; dispatcher concurrency is a single template parameter.
- **Static IP?** NAT gateway + Elastic IP per pool (about $32/month), selected per job.
- **Azure?** Same pattern: Storage Queue → Function → Container Apps Jobs → Blob. The client's
  `POOLS` routing already supports it.
- **What would you do next?** A job table (DynamoDB) for full status history, target splitting,
  auth on a hosted API.
