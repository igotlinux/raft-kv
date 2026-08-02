package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PersistentState is the small amount of Raft state that must survive a
// crash and be flushed to disk *before* replying to any RPC: currentTerm
// and votedFor. It is rewritten atomically (write-temp, fsync, rename) so
// a crash mid-write never leaves a corrupt file behind.
type PersistentState struct {
	CurrentTerm int    `json:"current_term"`
	VotedFor    string `json:"voted_for"` // peer id; "" if none
}

// Persister owns the on-disk write-ahead log (one JSON object per line,
// one per LogEntry) and the term/votedFor state file for a single node.
//
// Design notes (things worth saying out loud in an interview):
//   - The WAL is append-only during normal operation: appending N new
//     entries costs one write + one fsync, not O(N) rewrites.
//   - Raft occasionally needs to *truncate* the log (an AppendEntries
//     RPC discovers a conflicting entry and everything after it must be
//     discarded). A real system compacts via log segments; here that's
//     simplified to "rewrite the whole WAL file", which is O(log length)
//     but simple and still crash-safe because it goes through the same
//     write-temp+rename path.
//   - On startup, Recover() replays the WAL front-to-back to rebuild the
//     in-memory log, which is how a restarted/crashed node gets back to
//     where it left off before rejoining the cluster.
type Persister struct {
	mu        sync.Mutex
	dir       string
	wal       *os.File
	walPath   string
	statePath string
}

func NewPersister(dir string) (*Persister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("persister: mkdir: %w", err)
	}
	walPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("persister: open wal: %w", err)
	}
	return &Persister{
		dir:       dir,
		wal:       f,
		walPath:   walPath,
		statePath: filepath.Join(dir, "state.json"),
	}, nil
}

func (p *Persister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wal.Close()
}

// Recover reads whatever is on disk and reconstructs (state, log).
// Called once at startup, before the node accepts any RPCs.
func (p *Persister) Recover() (PersistentState, []LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := PersistentState{CurrentTerm: 0, VotedFor: ""}
	if b, err := os.ReadFile(p.statePath); err == nil {
		if err := json.Unmarshal(b, &state); err != nil {
			return state, nil, fmt.Errorf("persister: corrupt state file: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return state, nil, err
	}

	f, err := os.Open(p.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil, nil
		}
		return state, nil, err
	}
	defer f.Close()

	var log []LogEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e LogEntry
		if err := json.Unmarshal(line, &e); err != nil {
			// A torn write at the very end of the file (crash mid-append)
			// is expected and safe to ignore: that entry was never
			// acknowledged to the leader, so the leader will resend it.
			break
		}
		log = append(log, e)
	}
	return state, log, nil
}

// SaveState fsyncs currentTerm/votedFor to disk. Must be called before
// a node grants a vote or believes a term change, per the Raft paper.
func (p *Persister) SaveState(state PersistentState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tmp := p.statePath + ".tmp"
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, p.statePath)
}

// AppendEntries fsyncs new entries onto the tail of the WAL.
func (p *Persister) AppendEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	w := bufio.NewWriter(p.wal)
	enc := json.NewEncoder(w)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return p.wal.Sync()
}

// TruncateAndReplace rewrites the entire WAL to contain exactly `log`.
// Used when a conflicting entry forces the in-memory log to be cut back
// (see Raft.handleConflict). Goes through the same temp+rename+fsync
// dance as SaveState so a crash mid-rewrite can't corrupt the WAL.
func (p *Persister) TruncateAndReplace(log []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.wal.Close(); err != nil {
		return err
	}

	tmp := p.walPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, e := range log {
		if err := enc.Encode(e); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p.walPath); err != nil {
		return err
	}

	newF, err := os.OpenFile(p.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	p.wal = newF
	return nil
}
