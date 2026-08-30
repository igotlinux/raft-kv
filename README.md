# Fault-Tolerant Distributed Key-Value Store (Raft, Go)

A 5-node replicated key-value store implementing the Raft consensus
protocol from scratch: leader election, log replication, a WAL-backed
persistent log with crash recovery, containerized deployment, and a
fault-injection test suite covering 5 failure modes.

This README doubles as interview prep -- each section says not just
*what* was built but *why* it works and what its known trade-offs are.

## Architecture

```
                    ┌─────────────┐
   client  ───────► │ Client HTTP │  PUT / GET / DELETE / status
                    │  API :8000  │  (redirects to leader if wrong node)
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  KV Store   │  applies committed log entries
                    │ (state machine) │
                    └──────┬──────┘
                           │ ApplyMsg (committed entries, in order)
                    ┌──────▼──────┐
                    │  Raft Node  │  election, replication, commit index
                    └──────┬──────┘
                     ┌─────┴─────┐
              ┌──────▼───┐   ┌───▼──────┐
              │   WAL    │   │  HTTP    │  Raft RPC :7000
              │(disk)    │   │Transport │  (+ admin fault-injection API)
              └──────────┘   └──────────┘
```

- `internal/raft` -- the consensus engine. Transport- and
  persistence-agnostic (both are interfaces), which is what makes it unit
  testable with an in-memory fake network instead of real sockets.
- `internal/kv` -- the replicated state machine (a map) and the
  client-facing HTTP API.
- `internal/transport` -- the real HTTP implementation of `raft.Transport`,
  plus an admin API (`/admin/*`) to inject faults into a live process.
- `cmd/server` -- the binary; all cluster topology comes from env vars so
  the same image runs under Docker Compose or on EC2 unmodified.

## Quickstart (Docker Compose)

```bash
docker compose up --build
curl localhost:8001/status
curl -X POST localhost:8001/kv/put -d '{"key":"foo","value":"bar"}'
curl localhost:8003/kv/get?key=foo
```

## Quickstart (no Docker -- plain binaries)

```bash
go build -o raftkv-server ./cmd/server
PEERS="node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003,node4=127.0.0.1:7004,node5=127.0.0.1:7005"
CLIENTS="node1=127.0.0.1:8001,node2=127.0.0.1:8002,node3=127.0.0.1:8003,node4=127.0.0.1:8004,node5=127.0.0.1:8005"
NODE_ID=node1 PEERS=$PEERS CLIENT_ADDRS=$CLIENTS DATA_DIR=/tmp/n1 RAFT_LISTEN_ADDR=:7001 CLIENT_LISTEN_ADDR=:8001 ./raftkv-server &
# ...repeat for node2..node5 with matching ports/data dirs
```

## Design decisions worth explaining out loud

- **No-op on election** (§5.4.2 of the Raft paper): a new leader appends
  a no-op entry in its own term immediately, because Raft can't safely
  conclude an entry from a *previous* term is committed by replica count
  alone -- only entries from the leader's *current* term can be counted
  that way. Skipping this is a common, subtle correctness bug.
- **Fast conflict backtracking**: `AppendEntries` replies carry
  `ConflictIndex`/`ConflictTerm` so a lagging or rejoining follower
  doesn't need one RPC round-trip per conflicting log entry.
- **In-flight RPC guard**: only one `AppendEntries` is ever outstanding
  per peer at a time. Without it, replies can arrive out of order under
  jitter and a stale reply can regress `nextIndex`/`matchIndex` after a
  newer one already advanced them -- this was a real bug found while
  building this (see git history / test comments in `raft.go`).
- **WAL simplification**: the log is append-only during normal operation
  (one write + one fsync per batch), but a conflict-triggered truncation
  rewrites the whole file rather than using segment-based compaction like
  a production system (etcd, e.g.) would. Simpler, still crash-safe
  (write-temp + fsync + rename), and O(log length) instead of O(1) on the
  rare truncation path -- a deliberate scope trade-off.
- **Reads are local, not linearizable**: `GET` reads this node's local
  map directly. That's fast but can be stale if this node is a
  partitioned-away former leader or a follower that hasn't applied the
  latest entry yet. A production system would add a leader lease or
  ReadIndex check before serving a read. Flagged directly in
  `internal/kv/store.go`.

## Testing

```bash
go test ./internal/raft/... -v          # unit + fault-injection suite
go test ./internal/raft/... -race       # same, with the race detector
```

The suite (`internal/raft/raft_test.go`) uses an in-memory
fault-injectable network (`fake_network_test.go`, modeled on MIT 6.824's
"labrpc") to cover, deterministically and in well under a second:

| Failure mode | Test |
|---|---|
| Crashes | `TestCrashFailure` |
| Restarts (WAL replay) | `TestRestartRecovery` |
| Network partitions | `TestNetworkPartition` |
| RPC loss | `TestRPCLossAndDelay` |
| RPC delay | `TestRPCLossAndDelay` |

For the same 5 failure modes exercised against a **real, live** cluster
(Docker Compose or EC2) instead of in-process fakes:

```bash
./scripts/fault_injection.sh         # delay, loss, partition, crash, recover via HTTP admin API
./scripts/docker_crash_restart.sh    # a REAL `docker kill -9` + restart, forcing actual WAL replay
```

## Deployment

See `deploy/aws/README.md` for the full EC2 walkthrough (3 instances / 5
nodes, matching the resume's topology), including an honest discussion of
what fault tolerance that specific placement does and doesn't give you.

## Known limitations / explicitly out of scope

- No log compaction / snapshotting -- the log grows unboundedly. Adding
  it would mean periodically writing a KV-store snapshot plus the
  `(lastIncludedIndex, lastIncludedTerm)` it corresponds to, and truncating
  the WAL before that point; the *other* resume project (COW B+ tree
  snapshot engine) is a natural fit for the snapshot storage format here.
- No cluster membership changes (adding/removing nodes) -- Raft's joint
  consensus algorithm for that is a well-known follow-on extension.
- Reads are not linearizable (see above).
