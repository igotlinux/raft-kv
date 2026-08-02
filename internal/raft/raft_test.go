package raft

import (
	"testing"
	"time"
)

// TestLeaderElection: a fresh 5-node cluster should converge on exactly
// one leader.
func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	if _, ok := c.awaitLeader(2 * time.Second); !ok {
		t.Fatal("no leader elected")
	}
	c.checkNoSplitBrain()
}

// TestLogReplication: commands submitted to the leader must be committed
// on a majority and visible via the apply channel.
func TestLogReplication(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	if _, ok := c.awaitLeader(2 * time.Second); !ok {
		t.Fatal("no leader elected")
	}

	_, idx, ok := c.submit(Command{Op: OpPut, Key: "foo", Value: "bar"})
	if !ok {
		t.Fatal("leader rejected write")
	}
	if !c.waitCommittedOnMajority(idx, 2*time.Second) {
		t.Fatalf("index %d never committed on a majority", idx)
	}
}

// TestCrashFailure ("crashes"): killing the leader must not lose committed
// data and must result in a new leader being elected.
func TestCrashFailure(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	leaderID, ok := c.awaitLeader(2 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}
	_, idx, ok := c.submit(Command{Op: OpPut, Key: "k1", Value: "v1"})
	if !ok {
		t.Fatal("write rejected")
	}
	if !c.waitCommittedOnMajority(idx, 2*time.Second) {
		t.Fatal("write never committed before crash")
	}

	c.crash(leaderID)

	newLeader, ok := c.awaitLeader(2 * time.Second)
	if !ok {
		t.Fatal("no new leader elected after crash")
	}
	if newLeader == leaderID {
		t.Fatal("crashed node still reported as leader")
	}

	_, idx2, ok := c.submit(Command{Op: OpPut, Key: "k2", Value: "v2"})
	if !ok {
		t.Fatal("new leader rejected write")
	}
	if !c.waitCommittedOnMajority(idx2, 2*time.Second) {
		t.Fatal("cluster failed to make progress after crash")
	}
	c.checkNoSplitBrain()
}

// TestRestartRecovery ("restarts"): a crashed node, restarted, must
// recover currentTerm/votedFor/log from its WAL and rejoin correctly
// rather than e.g. re-voting for two candidates in a term it already
// voted in.
func TestRestartRecovery(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	leaderID, ok := c.awaitLeader(2 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}
	_, idx, ok := c.submit(Command{Op: OpPut, Key: "persisted", Value: "yes"})
	if !ok {
		t.Fatal("write rejected")
	}
	if !c.waitCommittedOnMajority(idx, 2*time.Second) {
		t.Fatal("write never committed")
	}

	// Crash a FOLLOWER (not the leader) so we can test WAL replay without
	// also triggering a new election.
	var follower string
	for _, id := range c.ids {
		if id != leaderID {
			follower = id
			break
		}
	}
	_, _, termBefore, _, _, logLenBefore := c.nodes[follower].Status()
	c.crash(follower)
	c.restart(follower)

	// Give it a moment to catch up on heartbeats.
	time.Sleep(300 * time.Millisecond)

	_, _, termAfter, _, _, logLenAfter := c.nodes[follower].Status()
	if termAfter < termBefore {
		t.Fatalf("recovered node lost term progress: before=%d after=%d", termBefore, termAfter)
	}
	if logLenAfter < logLenBefore {
		t.Fatalf("recovered node lost log entries: before=%d after=%d", logLenBefore, logLenAfter)
	}
	if _, ok := c.appliedValue(follower, idx); !ok {
		// It may need a beat longer to re-apply from its replayed log.
		time.Sleep(300 * time.Millisecond)
		if _, ok := c.appliedValue(follower, idx); !ok {
			t.Fatalf("restarted node never re-applied previously committed index %d", idx)
		}
	}
	c.checkNoSplitBrain()
}

// TestNetworkPartition ("network partitions"): split 5 nodes into a
// majority (3) and minority (2). The majority side must keep making
// progress; the minority side must NOT elect a leader (no split brain).
// After healing, the minority must catch up.
func TestNetworkPartition(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	origLeader, ok := c.awaitLeader(2 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}

	// Build the majority group around whichever node is currently
	// leader. (If we split arbitrarily, the *pre-existing* leader could
	// land in the minority side -- which is fine and expected in real
	// Raft: an isolated old leader keeps believing it's leader locally
	// since nothing has told it otherwise, it just can never commit
	// anything again. That's a leader-lease/stale-read concern, not the
	// property this test is checking, so we pin the setup to avoid it.)
	var majority, minority []string
	majority = append(majority, origLeader)
	for _, id := range c.ids {
		if id == origLeader {
			continue
		}
		if len(majority) < 3 {
			majority = append(majority, id)
		} else {
			minority = append(minority, id)
		}
	}
	c.net.Partition(1, majority)
	c.net.Partition(2, minority)

	// The majority side should elect/keep a leader and make progress.
	time.Sleep(300 * time.Millisecond)
	var majorityLeader string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range majority {
			_, state, _, _, _, _ := c.nodes[id].Status()
			if state == Leader {
				majorityLeader = id
			}
		}
		if majorityLeader != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if majorityLeader == "" {
		t.Fatal("majority partition failed to elect a leader")
	}

	idx, _, ok := c.nodes[majorityLeader].Start(Command{Op: OpPut, Key: "part", Value: "ition"})
	if !ok {
		t.Fatal("majority leader rejected write during partition")
	}
	if !c.waitCommittedOnMajority(idx, 2*time.Second) {
		t.Fatal("majority side failed to commit during partition")
	}

	// Minority side must not have a leader.
	for _, id := range minority {
		_, state, _, _, _, _ := c.nodes[id].Status()
		if state == Leader {
			t.Fatalf("minority node %s incorrectly became leader during partition", id)
		}
	}

	c.net.Heal()
	if !c.waitCommittedOnMajority(idx, 3*time.Second) {
		// Already true before heal via majority nodes, but after healing
		// the minority nodes should also converge; check the whole cluster.
	}
	time.Sleep(500 * time.Millisecond)
	for _, id := range minority {
		if _, ok := c.appliedValue(id, idx); !ok {
			t.Fatalf("minority node %s never caught up after heal", id)
		}
	}
	c.checkNoSplitBrain()
}

// TestRPCLossAndDelay ("RPC loss" and "delays"): even with a chunk of
// RPCs silently dropped and the rest delayed, the cluster must still
// converge on a single leader and keep committing writes -- just slower.
func TestRPCLossAndDelay(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	if _, ok := c.awaitLeader(2 * time.Second); !ok {
		t.Fatal("no leader elected before injecting faults")
	}

	c.net.SetDropRate(0.25)
	c.net.SetDelay(15 * time.Millisecond)
	defer func() {
		c.net.SetDropRate(0)
		c.net.SetDelay(0)
	}()

	committed := 0
	for i := 0; i < 5; i++ {
		_, idx, ok := c.submit(Command{Op: OpPut, Key: "chaos", Value: "test"})
		if !ok {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if c.waitCommittedOnMajority(idx, 3*time.Second) {
			committed++
		}
	}
	if committed == 0 {
		t.Fatal("cluster made zero progress under 25% RPC loss + delay")
	}
	c.checkNoSplitBrain()
}
