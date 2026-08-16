// Package transport implements raft.Transport over plain HTTP+JSON, and
// exposes an admin API to inject the same failure modes the in-memory
// FakeNetwork exercises in unit tests, but against real sockets/containers
// (see /admin/partition, /admin/droprate, /admin/delay). This is what the
// docker-compose fault-injection scripts drive.
package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"raftkv/internal/raft"
)

// PeerMap maps node id -> "host:port" for the internal Raft RPC port.
type PeerMap map[string]string

// FaultInjector holds the live, admin-toggleable fault configuration for
// one node's outbound RPCs. All fields are protected by mu so they can be
// flipped concurrently via the admin HTTP handlers while RPCs are in
// flight.
type FaultInjector struct {
	mu           sync.RWMutex
	droppedPeers map[string]bool // outbound RPCs to these peers are always dropped (simulated partition)
	dropRate     float64         // additional random drop probability applied to everyone
	delay        time.Duration
	crashed      bool // if true, this node drops ALL inbound+outbound traffic (simulated process crash)
}

func NewFaultInjector() *FaultInjector {
	return &FaultInjector{droppedPeers: make(map[string]bool)}
}

func (f *FaultInjector) SetPartition(peers []string, partitioned bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range peers {
		f.droppedPeers[p] = partitioned
	}
}

func (f *FaultInjector) SetDropRate(r float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropRate = r
}

func (f *FaultInjector) SetDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delay = d
}

func (f *FaultInjector) SetCrashed(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.crashed = v
}

func (f *FaultInjector) Snapshot() (partitioned []string, dropRate float64, delay time.Duration, crashed bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for p, v := range f.droppedPeers {
		if v {
			partitioned = append(partitioned, p)
		}
	}
	return partitioned, f.dropRate, f.delay, f.crashed
}

// shouldDrop decides, for one outbound call to `peer`, whether to drop it.
func (f *FaultInjector) shouldDrop(peer string) (drop bool, delay time.Duration) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.crashed {
		return true, 0
	}
	if f.droppedPeers[peer] {
		return true, 0
	}
	if f.dropRate > 0 && rand.Float64() < f.dropRate {
		return true, 0
	}
	return false, f.delay
}

// HTTPTransport implements raft.Transport by POSTing JSON to peers.
type HTTPTransport struct {
	self   string
	peers  PeerMap
	client *http.Client
	faults *FaultInjector
}

func NewHTTPTransport(self string, peers PeerMap, faults *FaultInjector) *HTTPTransport {
	return &HTTPTransport{
		self:   self,
		peers:  peers,
		client: &http.Client{Timeout: raft.RPCTimeout},
		faults: faults,
	}
}

func (t *HTTPTransport) post(peer, path string, body interface{}, out interface{}) error {
	if drop, delay := t.faults.shouldDrop(peer); drop {
		return fmt.Errorf("transport: simulated fault dropped RPC to %s", peer)
	} else if delay > 0 {
		time.Sleep(delay)
	}

	addr, ok := t.peers[peer]
	if !ok {
		return fmt.Errorf("transport: unknown peer %q", peer)
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s%s", addr, path)
	resp, err := t.client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(respBody, out)
}

func (t *HTTPTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	if err := t.post(peer, "/raft/request_vote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	if err := t.post(peer, "/raft/append_entries", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// RegisterRaftHandlers wires up the inbound RPC endpoints a peer calls on
// this node. Inbound calls are ALSO subject to the crashed flag (a truly
// crashed process answers nothing), but not to the outbound-only
// partition/drop-rate settings, which model this node's *view* of the
// network rather than physical severance -- matching how the FakeNetwork
// test harness treats "from is offline" vs "from->to partitioned".
func RegisterRaftHandlers(mux *http.ServeMux, node *raft.Node, faults *FaultInjector) {
	mux.HandleFunc("/raft/request_vote", func(w http.ResponseWriter, r *http.Request) {
		if _, _, _, crashed := faults.Snapshot(); crashed {
			http.Error(w, "node crashed", http.StatusServiceUnavailable)
			return
		}
		var args raft.RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleRequestVote(&args)
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/append_entries", func(w http.ResponseWriter, r *http.Request) {
		if _, _, _, crashed := faults.Snapshot(); crashed {
			http.Error(w, "node crashed", http.StatusServiceUnavailable)
			return
		}
		var args raft.AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := node.HandleAppendEntries(&args)
		json.NewEncoder(w).Encode(reply)
	})
}
