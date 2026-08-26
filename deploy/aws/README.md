# Deploying the 5-node cluster to AWS EC2

This mirrors the resume bullet: *"Containerized and deployed the 5-node
cluster across 3 AWS EC2 instances."* Five Raft nodes across three
machines means at least one instance runs two nodes -- which is exactly
the kind of realistic constraint worth being ready to explain: it's fine
because Raft's unit of fault tolerance is the *node* (process +
its own disk), not the *machine*; running two nodes on one instance just
means that instance's failure takes down two replicas at once instead of
one, which changes your fault-tolerance math (see "Placement" below) but
not the correctness of the protocol.

## Topology

| EC2 instance | Nodes it runs   |
|---------------|-----------------|
| instance-a    | node1, node2    |
| instance-b    | node3, node4    |
| instance-c    | node5           |

## 1. Provision instances

```bash
# Cheapest reasonable option for a demo: 3x t3.small (2 vCPU/2GB), Ubuntu 24.04.
aws ec2 run-instances \
  --image-id ami-0XXXXXXXXXXXXXXXX \       # Ubuntu 24.04 LTS AMI for your region
  --count 3 \
  --instance-type t3.small \
  --key-name YOUR_KEY_PAIR \
  --security-group-ids sg-XXXXXXXX \       # see security group rules below
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=raftkv}]'
```

Security group inbound rules needed:
- TCP 7000-7010 (Raft internal RPC + admin) from the other cluster instances' security group / private IPs only -- never from 0.0.0.0/0.
- TCP 8000-8010 (client API) from wherever your clients / your own IP are.
- TCP 22 (SSH) from your IP.

## 2. Install Docker on each instance

```bash
ssh ubuntu@<instance-ip> 'curl -fsSL https://get.docker.com | sudo sh && sudo usermod -aG docker ubuntu'
```

## 3. Ship the image

Simplest path for a demo-scale deploy: build once, save, `scp`, load.

```bash
docker build -t raftkv-server:latest .
docker save raftkv-server:latest | gzip > raftkv-server.tar.gz
for host in instance-a instance-b instance-c; do
  scp raftkv-server.tar.gz ubuntu@$host:~/
  ssh ubuntu@$host 'gunzip -c raftkv-server.tar.gz | docker load'
done
```
(For anything beyond a demo, push to ECR instead and `docker pull` on each
host -- see `push_to_ecr.sh` below.)

## 4. Run each node

Every node needs the SAME `PEERS`/`CLIENT_ADDRS` maps, pointing at the
instances' **private IPs** (put all 3 instances in the same VPC/subnet).
Substitute real private IPs for `10.0.0.11/12/13` below.

```bash
PEERS="node1=10.0.0.11:7001,node2=10.0.0.11:7002,node3=10.0.0.12:7003,node4=10.0.0.12:7004,node5=10.0.0.13:7005"
CLIENTS="node1=10.0.0.11:8001,node2=10.0.0.11:8002,node3=10.0.0.12:8003,node4=10.0.0.12:8004,node5=10.0.0.13:8005"

# on instance-a:
docker run -d --name node1 -e NODE_ID=node1 -e PEERS="$PEERS" -e CLIENT_ADDRS="$CLIENTS" \
  -e RAFT_LISTEN_ADDR=:7001 -e CLIENT_LISTEN_ADDR=:8001 \
  -p 7001:7001 -p 8001:8001 -v node1-data:/data raftkv-server:latest
docker run -d --name node2 -e NODE_ID=node2 -e PEERS="$PEERS" -e CLIENT_ADDRS="$CLIENTS" \
  -e RAFT_LISTEN_ADDR=:7002 -e CLIENT_LISTEN_ADDR=:8002 \
  -p 7002:7002 -p 8002:8002 -v node2-data:/data raftkv-server:latest

# on instance-b: node3, node4 (same pattern, ports 7003/8003, 7004/8004)
# on instance-c: node5 (port 7005/8005)
```

`deploy.sh` in this directory automates the above over SSH given a hosts
file; see the top of that script for usage.

## 5. Verify

```bash
curl http://<instance-a-public-ip>:8001/status
curl -X POST http://<instance-a-public-ip>:8001/kv/put -d '{"key":"hello","value":"aws"}'
curl http://<instance-c-public-ip>:8005/kv/get?key=hello
```

## Placement and fault tolerance, worth saying out loud in an interview

Raft with 5 nodes tolerates 2 simultaneous node failures (needs a
majority of 3 alive). But if node1+node2 both live on instance-a, losing
that ONE instance takes out 2 nodes at once -- so this 3-instance layout
can only truly tolerate the loss of **one whole instance** (worst case,
instance-a or instance-b, taking 2 nodes with it), not two independent
node failures. A production deployment would put each node on its own
instance/AZ; 3 instances for 5 nodes here is a deliberate cost/realism
trade-off for a portfolio deployment, not a recommended production
topology -- and calling that out unprompted is usually a good signal in
an interview.

## Cleanup

```bash
for host in instance-a instance-b instance-c; do
  ssh ubuntu@$host 'docker rm -f $(docker ps -aq --filter name=node) 2>/dev/null'
done
```
