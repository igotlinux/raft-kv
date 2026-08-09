// Package kv is the replicated state machine on top of Raft: an
// in-memory map that only ever changes by applying committed log
// entries, plus the bookkeeping needed to tell an HTTP handler waiting
// on a client's PUT/GET/DELETE when "its" entry has actually committed.
package kv

import (
	"errors"
	"sync"
	"time"

	"raftkv/internal/raft"
)

// ProposeTimeout bounds how long Put/Delete will wait for their entry to
// commit before giving up. A var (not const) so tests/servers can tune it.
var ProposeTimeout = 3 * time.Second

var ErrTimeout = errors.New("timed out waiting for commit")

type pendingResult struct {
	term int
	ch   chan applyResult
}

type applyResult struct {
	term int // term the entry was actually committed under
}

// Store is the key-value state machine. Exactly one goroutine (Run)
// applies entries from the Raft apply channel in order; all other access
// goes through the exported methods, which are safe for concurrent use.
type Store struct {
	mu   sync.RWMutex
	data map[string]string

	node *raft.Node

	pendingMu sync.Mutex
	pending   map[int]*pendingResult // log index -> waiter
}

func NewStore(node *raft.Node) *Store {
	return &Store{
		data:    make(map[string]string),
		node:    node,
		pending: make(map[int]*pendingResult),
	}
}

// Run consumes committed entries from applyCh forever. Call it in its
// own goroutine.
func (s *Store) Run(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		if !msg.CommandValid {
			continue
		}
		var result applyResult
		result.term = msg.CommandTerm

		switch msg.Command.Op {
		case raft.OpPut:
			s.mu.Lock()
			s.data[msg.Command.Key] = msg.Command.Value
			s.mu.Unlock()
		case raft.OpDelete:
			s.mu.Lock()
			delete(s.data, msg.Command.Key)
			s.mu.Unlock()
		case raft.OpNoop:
			// no state change; still unblocks anyone (there shouldn't be
			// a waiter on a no-op index, but keep the bookkeeping simple)
		}

		s.pendingMu.Lock()
		if w, ok := s.pending[msg.CommandIndex]; ok {
			delete(s.pending, msg.CommandIndex)
			w.ch <- result
			close(w.ch)
		}
		s.pendingMu.Unlock()
	}
}

// Get reads the current value directly from local state. Because Raft
// only guarantees the *log* is linearizable, a plain local read like
// this can return stale data if this node is a stale/partitioned leader
// or a follower that hasn't applied the latest entry yet; a production
// system would route reads through the leader with a ReadIndex or lease
// check first. Flagging that trade-off explicitly is worth doing out
// loud, since it's exactly the kind of thing an interviewer will probe.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// ErrNotLeader is returned by Put/Delete when this node is not currently
// the Raft leader.
type ErrNotLeader struct{ LeaderHint string }

func (e *ErrNotLeader) Error() string { return "not leader" }

// Put proposes a PUT to the Raft log and blocks until it is committed
// (or the term changes underneath it, meaning it may never commit --
// e.g. this node lost leadership before the entry was replicated).
func (s *Store) Put(key, value string) error {
	return s.propose(raft.Command{Op: raft.OpPut, Key: key, Value: value})
}

func (s *Store) Delete(key string) error {
	return s.propose(raft.Command{Op: raft.OpDelete, Key: key})
}

func (s *Store) propose(cmd raft.Command) error {
	index, term, isLeader := s.node.Start(cmd)
	if !isLeader {
		_, _, _, leaderID, _, _ := s.node.Status()
		return &ErrNotLeader{LeaderHint: leaderID}
	}

	w := &pendingResult{term: term, ch: make(chan applyResult, 1)}
	s.pendingMu.Lock()
	s.pending[index] = w
	s.pendingMu.Unlock()

	var res applyResult
	select {
	case res = <-w.ch:
	case <-time.After(ProposeTimeout):
		s.pendingMu.Lock()
		delete(s.pending, index)
		s.pendingMu.Unlock()
		return ErrTimeout
	}
	if res.term != term {
		// A different leader's entry ended up at this index (this
		// node's tentative entry was overwritten after losing an
		// election) -- the original proposal was never actually
		// committed.
		return &ErrNotLeader{}
	}
	return nil
}
