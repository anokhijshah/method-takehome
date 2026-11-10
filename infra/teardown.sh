#!/usr/bin/env bash
# Delete an agent pool stack and its retained buckets (results are lost).
#   ./infra/teardown.sh [stack-name]
set -euo pipefail
STACK="${1:-cloud-agent}"
export AWS_REGION="${AWS_REGION:-us-east-2}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

read -r -p "Delete stack '$STACK' and ALL scan results in account $ACCOUNT? [y/N] " ok
[[ "$ok" == "y" ]] || exit 1

RESULTS=$(aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='ResultsBucketName'].OutputValue" --output text 2>/dev/null || true)
aws cloudformation delete-stack --stack-name "$STACK"
aws cloudformation wait stack-delete-complete --stack-name "$STACK"
echo "Stack deleted"

empty_and_delete() {  # versioned buckets need every version removed
  local b="$1"; aws s3api head-bucket --bucket "$b" >/dev/null 2>&1 || return 0
  python3 - "$b" <<'PY'
import sys, subprocess, json
b = sys.argv[1]
while True:
    out = json.loads(subprocess.check_output(["aws", "s3api", "list-object-versions", "--bucket", b, "--max-items", "1000", "--output", "json"]) or "{}")
    objs = [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in out.get("Versions", []) + out.get("DeleteMarkers", [])]
    if not objs: break
    subprocess.check_call(["aws", "s3api", "delete-objects", "--bucket", b, "--delete", json.dumps({"Objects": objs, "Quiet": True})])
PY
  aws s3api delete-bucket --bucket "$b" && echo "Deleted bucket $b"
}
[[ -n "$RESULTS" && "$RESULTS" != "None" ]] && empty_and_delete "$RESULTS"
empty_and_delete "${STACK}-artifacts-${ACCOUNT}-${AWS_REGION}"
