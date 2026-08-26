#!/bin/sh
# Push raftkv-server:latest to a private ECR repo, for deployments beyond
# the scp/docker-save demo path in README.md.
#
# Usage: ./push_to_ecr.sh <aws-account-id> <region> [repo-name]
set -eu

ACCOUNT_ID="${1:?AWS account id required}"
REGION="${2:?AWS region required, e.g. us-east-1}"
REPO="${3:-raftkv-server}"

ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO"

aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1 \
  || aws ecr create-repository --repository-name "$REPO" --region "$REGION"

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

docker build -t "$REPO:latest" .
docker tag "$REPO:latest" "$ECR_URI:latest"
docker push "$ECR_URI:latest"

echo "Pushed. On each EC2 instance:"
echo "  aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"
echo "  docker pull $ECR_URI:latest"
