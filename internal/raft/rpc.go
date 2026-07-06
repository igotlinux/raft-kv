package raft

// RequestVoteArgs is sent by candidates to gather votes.
type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs is sent by the leader both to replicate log entries
// and, with an empty Entries slice, as a heartbeat.
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`

	// Fast log backtracking (conflict optimization) so a lagging/rejoining
	// follower doesn't need one RPC round-trip per conflicting entry.
	ConflictIndex int `json:"conflict_index"`
	ConflictTerm  int `json:"conflict_term"`
	LogLen        int `json:"log_len"`
}

// Transport abstracts how one node talks to another so the core Raft
// logic can be tested with an in-memory fake network (see raft_test.go)
// and run for real over HTTP (see internal/transport).
type Transport interface {
	SendRequestVote(peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}
