package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

// FakeNetwork is an in-process stand-in for the real HTTP transport,
// modelled on the "labrpc" approach used in MIT 6.824: it lets tests
// deterministically inject the failure modes real distributed systems
// have to survive, without touching real sockets or containers.
//
//   - crash a node: MarkOffline (it stops responding to RPCs, but its
//     Persister -- a real on-disk file in a temp dir -- keeps whatever
//     was fsynced, exactly like a killed process)
//   - restart a node: MarkOnline plus reconstructing a fresh *Node from
//     the same Persister directory, which replays the WAL
//   - network partition: Partition splits the cluster into groups that
//     cannot reach each other
//   - RPC loss: DropRate randomly fails a fraction of RPCs
//   - RPC delay: Delay adds latency before a call completes
type FakeNetwork struct {
	mu         sync.Mutex
	nodes      map[string]*Node
	online     map[string]bool
	partitions map[string]int // node id -> partition group id; 0 = default group, all default-group nodes can reach each other
	dropRate   float64
	delay      time.Duration
	rng        *rand.Rand
}

func NewFakeNetwork() *FakeNetwork {
	return &FakeNetwork{
		nodes:      make(map[string]*Node),
		online:     make(map[string]bool),
		partitions: make(map[string]int),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (fn *FakeNetwork) Register(id string, n *Node) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.nodes[id] = n
	fn.online[id] = true
	fn.partitions[id] = 0
}

func (fn *FakeNetwork) MarkOffline(id string) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.online[id] = false
}

func (fn *FakeNetwork) MarkOnline(id string, n *Node) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.nodes[id] = n // may be a freshly-recovered *Node after "restart"
	fn.online[id] = true
}

// Partition assigns each node in `group` to partition number `groupID`.
// Nodes only reach others in the same groupID. Call Heal to restore full
// connectivity.
func (fn *FakeNetwork) Partition(groupID int, group []string) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for _, id := range group {
		fn.partitions[id] = groupID
	}
}

func (fn *FakeNetwork) Heal() {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for id := range fn.partitions {
		fn.partitions[id] = 0
	}
}

func (fn *FakeNetwork) SetDropRate(r float64) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.dropRate = r
}

func (fn *FakeNetwork) SetDelay(d time.Duration) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	fn.delay = d
}

// reachable decides, for this single call, whether `from` can currently
// talk to `to`, and returns the target node plus any artificial delay.
func (fn *FakeNetwork) reachable(from, to string) (*Node, time.Duration, error) {
	fn.mu.Lock()
	defer fn.mu.Unlock()

	if !fn.online[from] || !fn.online[to] {
		return nil, 0, errors.New("node offline")
	}
	if fn.partitions[from] != fn.partitions[to] {
		return nil, 0, errors.New("network partitioned")
	}
	if fn.dropRate > 0 && fn.rng.Float64() < fn.dropRate {
		return nil, 0, errors.New("rpc dropped")
	}
	node, ok := fn.nodes[to]
	if !ok {
		return nil, 0, errors.New("unknown node")
	}
	return node, fn.delay, nil
}

// FakeTransport implements raft.Transport for one node, routing every
// call through the shared FakeNetwork so it is subject to whatever
// faults the test has configured.
type FakeTransport struct {
	net  *FakeNetwork
	from string
}

func NewFakeTransport(net *FakeNetwork, from string) *FakeTransport {
	return &FakeTransport{net: net, from: from}
}

func (t *FakeTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	node, delay, err := t.net.reachable(t.from, peer)
	if err != nil {
		return nil, err
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return node.HandleRequestVote(args), nil
}

func (t *FakeTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	node, delay, err := t.net.reachable(t.from, peer)
	if err != nil {
		return nil, err
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return node.HandleAppendEntries(args), nil
}
