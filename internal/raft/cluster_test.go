package raft

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// cluster wires N real Node instances (each with a real on-disk Persister
// in a temp dir) together over a FakeNetwork, so tests exercise the same
// code path that runs in production, only swapping HTTP sockets for an
// in-memory network the test can break on purpose.
type cluster struct {
	t        *testing.T
	net      *FakeNetwork
	ids      []string
	dirs     map[string]string
	nodes    map[string]*Node
	applyChs map[string]chan ApplyMsg

	mu      sync.Mutex
	applied map[string]map[int]ApplyMsg // node id -> committed index -> msg
	crashed map[string]bool
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	// Fast timings so tests don't take real-world minutes.
	MinElectionTimeout = 60 * time.Millisecond
	MaxElectionTimeout = 120 * time.Millisecond
	HeartbeatInterval = 20 * time.Millisecond

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node%d", i+1)
	}
	c := &cluster{
		t:        t,
		net:      NewFakeNetwork(),
		ids:      ids,
		dirs:     make(map[string]string),
		nodes:    make(map[string]*Node),
		applyChs: make(map[string]chan ApplyMsg),
		applied:  make(map[string]map[int]ApplyMsg),
		crashed:  make(map[string]bool),
	}
	for _, id := range ids {
		dir, err := os.MkdirTemp("", "raft-"+id+"-")
		if err != nil {
			t.Fatal(err)
		}
		c.dirs[id] = dir
		c.applied[id] = make(map[int]ApplyMsg)
		c.start(id)
	}
	return c
}

func (c *cluster) start(id string) {
	persister, err := NewPersister(c.dirs[id])
	if err != nil {
		c.t.Fatal(err)
	}
	applyCh := make(chan ApplyMsg, 100)
	transport := NewFakeTransport(c.net, id)
	logf := func(string, ...interface{}) {}
	if os.Getenv("RAFT_TEST_VERBOSE") != "" {
		t := c.t
		logf = func(format string, args ...interface{}) { t.Logf(format, args...) }
	}
	node, err := NewNode(id, c.ids, transport, persister, applyCh, logf)
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = node
	c.applyChs[id] = applyCh
	c.net.Register(id, node)
	go node.Run()
	go c.recordApplies(id, applyCh)
}

func (c *cluster) recordApplies(id string, ch chan ApplyMsg) {
	for msg := range ch {
		c.mu.Lock()
		c.applied[id][msg.CommandIndex] = msg
		c.mu.Unlock()
	}
}

// crash simulates `kill -9`: the node stops answering RPCs immediately.
// Its WAL/state file on disk are left exactly as last fsynced.
func (c *cluster) crash(id string) {
	c.mu.Lock()
	c.crashed[id] = true
	c.mu.Unlock()
	c.net.MarkOffline(id)
	c.nodes[id].Stop()
}

// restart simulates the process being relaunched: a brand new *Node is
// built from the same on-disk directory, which replays the WAL.
func (c *cluster) restart(id string) {
	c.mu.Lock()
	c.crashed[id] = false
	c.mu.Unlock()
	c.start(id)
}

func (c *cluster) shutdown() {
	for _, id := range c.ids {
		c.nodes[id].Stop()
		os.RemoveAll(c.dirs[id])
	}
}

// leader polls all nodes and returns one that currently believes it's
// the leader, if any (does not guarantee uniqueness by itself -- callers
// combine this with checkOneLeader for that).
// leader only considers currently-reachable (not crashed) nodes: a
// crashed process's in-memory state is unobservable in a real system
// until it comes back and starts answering RPCs again, so the test
// harness shouldn't "cheat" by reading it directly either.
func (c *cluster) leader() (string, bool) {
	for _, id := range c.ids {
		c.mu.Lock()
		down := c.crashed[id]
		c.mu.Unlock()
		if down {
			continue
		}
		if _, ok := c.nodes[id]; !ok {
			continue
		}
		_, state, _, _, _, _ := c.nodes[id].Status()
		if state == Leader {
			return id, true
		}
	}
	return "", false
}

// awaitLeader polls until exactly one leader is visible (the common case;
// during elections it may briefly be zero) or the timeout elapses.
func (c *cluster) awaitLeader(timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id, ok := c.leader(); ok {
			return id, true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", false
}

// checkNoSplitBrain fails the test if two nodes claim leadership in the
// same term simultaneously -- the core Raft safety property.
func (c *cluster) checkNoSplitBrain() {
	c.t.Helper()
	termToLeaders := make(map[int][]string)
	for _, id := range c.ids {
		c.mu.Lock()
		down := c.crashed[id]
		c.mu.Unlock()
		if down {
			continue
		}
		n, ok := c.nodes[id]
		if !ok {
			continue
		}
		_, state, term, _, _, _ := n.Status()
		if state == Leader {
			termToLeaders[term] = append(termToLeaders[term], id)
		}
	}
	for term, leaders := range termToLeaders {
		if len(leaders) > 1 {
			c.t.Fatalf("split brain: term %d has multiple leaders: %v", term, leaders)
		}
	}
}

// submit sends a command to whichever node currently looks like the
// leader and returns the log index it was assigned.
func (c *cluster) submit(cmd Command) (id string, index int, ok bool) {
	leaderID, found := c.leader()
	if !found {
		return "", -1, false
	}
	idx, _, isLeader := c.nodes[leaderID].Start(cmd)
	return leaderID, idx, isLeader
}

// waitCommittedOnMajority waits until at least a majority of *currently
// online* nodes report the given index as applied, which is the
// externally-observable definition of "committed".
func (c *cluster) waitCommittedOnMajority(index int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	majority := len(c.ids)/2 + 1
	for time.Now().Before(deadline) {
		count := 0
		c.mu.Lock()
		for _, id := range c.ids {
			if _, ok := c.applied[id][index]; ok {
				count++
			}
		}
		c.mu.Unlock()
		if count >= majority {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (c *cluster) appliedValue(id string, index int) (ApplyMsg, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.applied[id][index]
	return m, ok
}
