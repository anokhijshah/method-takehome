#!/usr/bin/env bash
# Deploy (or update) one agent pool stack.
#   ./infra/deploy.sh [stack-name]          default stack: cloud-agent
# Env: AWS_PROFILE, AWS_REGION (default us-east-2), VPC_ID / SUBNET_IDS (default: default VPC)
set -euo pipefail
cd "$(dirname "$0")"

STACK="${1:-cloud-agent}"
export AWS_REGION="${AWS_REGION:-us-east-2}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ARTIFACTS="${STACK}-artifacts-${ACCOUNT}-${AWS_REGION}"

VPC_ID="${VPC_ID:-$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)}"
SUBNET_IDS="${SUBNET_IDS:-$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC_ID" Name=map-public-ip-on-launch,Values=true \
  --query 'Subnets[].SubnetId' --output text | tr '\t' ',')}"
[[ "$VPC_ID" == "None" || -z "$SUBNET_IDS" ]] && { echo "No VPC/public subnets found; set VPC_ID and SUBNET_IDS" >&2; exit 1; }

# Private bucket for the packaged Lambda code.
if ! aws s3api head-bucket --bucket "$ARTIFACTS" >/dev/null 2>&1; then
  echo "Creating artifacts bucket $ARTIFACTS"
  aws s3api create-bucket --bucket "$ARTIFACTS" --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null
  aws s3api put-public-access-block --bucket "$ARTIFACTS" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
fi

PACKAGED=$(mktemp -t packaged.XXXXXX)
aws cloudformation package --template-file template.yaml --s3-bucket "$ARTIFACTS" --s3-prefix lambda \
  --output-template-file "$PACKAGED" >/dev/null
aws cloudformation validate-template --template-body "file://$PACKAGED" >/dev/null && echo "Template valid"

aws cloudformation deploy --stack-name "$STACK" --template-file "$PACKAGED" \
  --capabilities CAPABILITY_IAM --no-fail-on-empty-changeset \
  --parameter-overrides VpcId="$VPC_ID" SubnetIds="$SUBNET_IDS" \
  --tags project=method-cloud-agent

aws cloudformation describe-stacks --stack-name "$STACK" --query 'Stacks[0].Outputs' --output table
