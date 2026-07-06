package raft

import "time"

// State is one of the three Raft roles.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Command is the payload a client wants applied to the replicated
// key-value state machine.
type Command struct {
	Op    string `json:"op"` // "PUT", "DELETE", "NOOP"
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

const (
	OpPut    = "PUT"
	OpDelete = "DELETE"
	OpNoop   = "NOOP" // committed by every new leader to commit prior-term entries
)

// LogEntry is a single entry in the replicated log. Index is 1-based;
// index 0 is a sentinel "nothing committed yet" entry.
type LogEntry struct {
	Term    int     `json:"term"`
	Index   int     `json:"index"`
	Command Command `json:"command"`
}

// ApplyMsg is delivered to the state machine once an entry is committed
// by a majority of the cluster and safe to apply.
type ApplyMsg struct {
	CommandValid bool
	Command      Command
	CommandIndex int
	CommandTerm  int
}

// Timing. Kept as vars (not const) so tests / fault injection can tune them.
var (
	MinElectionTimeout = 300 * time.Millisecond
	MaxElectionTimeout = 600 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
	RPCTimeout         = 250 * time.Millisecond
)
