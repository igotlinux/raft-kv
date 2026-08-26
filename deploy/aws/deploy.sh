#!/bin/sh
# Deploys the 5-node cluster across 3 EC2 instances over SSH, following
# the topology in README.md: instance-a runs node1+node2, instance-b runs
# node3+node4, instance-c runs node5.
#
# Usage:
#   ./deploy.sh <instance-a-ip> <instance-b-ip> <instance-c-ip> <ssh-user>
#
# Prerequisites:
#   - Docker already installed on all 3 hosts (see README.md step 2)
#   - raftkv-server:latest image already loaded/pulled on all 3 hosts
#   - You can SSH to each host with your default key (or set SSH_KEY env var)
set -eu

A_IP="${1:?instance-a private/public IP required}"
B_IP="${2:?instance-b private/public IP required}"
C_IP="${3:?instance-c private/public IP required}"
USER="${4:-ubuntu}"
SSH_OPTS="${SSH_KEY:+-i $SSH_KEY}"

PEERS="node1=$A_IP:7001,node2=$A_IP:7002,node3=$B_IP:7003,node4=$B_IP:7004,node5=$C_IP:7005"
CLIENTS="node1=$A_IP:8001,node2=$A_IP:8002,node3=$B_IP:8003,node4=$B_IP:8004,node5=$C_IP:8005"

run_node() {
  host="$1"; id="$2"; rport="$3"; cport="$4"
  echo "-- starting $id on $host --"
  # shellcheck disable=SC2029
  ssh $SSH_OPTS "$USER@$host" "docker rm -f $id 2>/dev/null; \
    docker run -d --name $id \
      -e NODE_ID=$id -e PEERS='$PEERS' -e CLIENT_ADDRS='$CLIENTS' \
      -e RAFT_LISTEN_ADDR=:$rport -e CLIENT_LISTEN_ADDR=:$cport \
      -p $rport:$rport -p $cport:$cport \
      -v ${id}-data:/data raftkv-server:latest"
}

run_node "$A_IP" node1 7001 8001
run_node "$A_IP" node2 7002 8002
run_node "$B_IP" node3 7003 8003
run_node "$B_IP" node4 7004 8004
run_node "$C_IP" node5 7005 8005

echo ""
echo "Deployed. Verify with:"
echo "  curl http://$A_IP:8001/status"
