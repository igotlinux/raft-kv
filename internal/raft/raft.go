// Package raft implements the core consensus algorithm from the Raft
// paper (Ongaro & Ousterhout, "In Search of an Understandable Consensus
// Algorithm"): leader election, log replication via AppendEntries, and
// commit-index advancement by majority match. It is transport-agnostic
// (see the Transport interface in rpc.go) and persistence-agnostic
// beyond the Persister interface it's handed, which is what makes it
// unit-testable with an in-memory fake network instead of real sockets.
package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Node is one replica in the Raft cluster.
type Node struct {
	mu sync.Mutex

	id        string
	peerAddrs []string // ids of ALL nodes in the cluster, including self
	transport Transport
	persister *Persister

	// --- persistent state (must be fsynced before responding to RPCs
	// that depend on it) ---
	currentTerm int
	votedFor    string     // "" == none
	log         []LogEntry // log[0] is a sentinel with Index 0, Term 0

	// --- volatile state on all servers ---
	state       State
	commitIndex int
	lastApplied int
	leaderID    string // best-known current leader, for client redirects

	// --- volatile state on leaders only (reinitialized on election) ---
	nextIndex  map[string]int
	matchIndex map[string]int
	// inFlight prevents a second AppendEntries to the same peer from
	// being sent while one is still outstanding. Without this, replies
	// can arrive out of order under delay/jitter, and a stale reply can
	// regress nextIndex/matchIndex after a newer one already advanced
	// them -- a classic source of subtle Raft implementation bugs.
	inFlight map[string]bool

	applyCh        chan ApplyMsg
	electionResetC chan struct{}
	stopped        bool
	stopc          chan struct{}
	wg             sync.WaitGroup

	logger func(format string, args ...interface{})
}

// NewNode constructs a node and immediately recovers any state left on
// disk from a previous run (crash recovery). It does not start any
// timers; call Run() for that.
func NewNode(id string, peerAddrs []string, transport Transport, persister *Persister, applyCh chan ApplyMsg, logf func(string, ...interface{})) (*Node, error) {
	state, log, err := persister.Recover()
	if err != nil {
		return nil, err
	}
	if len(log) == 0 {
		log = []LogEntry{{Term: 0, Index: 0, Command: Command{Op: OpNoop}}}
	}
	if logf == nil {
		logf = func(string, ...interface{}) {}
	}
	n := &Node{
		id:             id,
		peerAddrs:      peerAddrs,
		transport:      transport,
		persister:      persister,
		currentTerm:    state.CurrentTerm,
		votedFor:       state.VotedFor,
		log:            log,
		state:          Follower,
		commitIndex:    0,
		lastApplied:    0,
		applyCh:        applyCh,
		electionResetC: make(chan struct{}, 1),
		stopc:          make(chan struct{}),
		logger:         logf,
	}
	n.logger("node %s recovered: term=%d votedFor=%q logLen=%d", id, n.currentTerm, n.votedFor, len(n.log)-1)
	return n, nil
}

// Run starts the election timer and the apply loop as background
// goroutines and returns immediately; callers (main, or a test harness)
// keep the process/goroutine alive independently and call Stop() to shut
// everything down cleanly.
func (n *Node) Run() {
	n.wg.Add(2)
	go func() {
		defer n.wg.Done()
		n.applyLoop()
	}()
	go func() {
		defer n.wg.Done()
		n.runElectionTimer()
	}()
}

// Stop signals every goroutine owned by this node to exit and blocks
// until they actually have, so that a caller (e.g. a test simulating a
// process restart, or a real shutdown handler) can be sure no stale
// goroutine is still touching shared state after Stop returns.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopc)
	n.mu.Unlock()
	n.wg.Wait()
}

func randomElectionTimeout() time.Duration {
	span := MaxElectionTimeout - MinElectionTimeout
	return MinElectionTimeout + time.Duration(rand.Int63n(int64(span)))
}

// --- election timer -------------------------------------------------

func (n *Node) runElectionTimer() {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.stopc:
			return
		case <-n.electionResetC:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(randomElectionTimeout())
		case <-timer.C:
			n.mu.Lock()
			isLeader := n.state == Leader
			n.mu.Unlock()
			if !isLeader {
				n.startElection()
			}
			timer.Reset(randomElectionTimeout())
		}
	}
}

func (n *Node) resetElectionTimer() {
	select {
	case n.electionResetC <- struct{}{}:
	default:
	}
}

// --- leader election --------------------------------------------------

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastIdx, lastTerm := n.lastLogIndexAndTerm()
	_ = n.persister.SaveState(PersistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor})
	n.logger("node %s starting election for term %d", n.id, term)
	n.mu.Unlock()

	votes := 1 // vote for self
	var votesMu sync.Mutex
	majority := len(n.peerAddrs)/2 + 1
	done := make(chan struct{})
	becameLeader := false

	for _, peer := range n.peerAddrs {
		if peer == n.id {
			continue
		}
		go func(peer string) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply, err := n.transport.SendRequestVote(peer, args)
			if err != nil || reply == nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if n.state != Candidate || n.currentTerm != term {
				return // stale reply from a prior term / already moved on
			}
			if reply.VoteGranted {
				votesMu.Lock()
				votes++
				v := votes
				votesMu.Unlock()
				if v >= majority && !becameLeader {
					becameLeader = true
					n.becomeLeaderLocked()
					close(done)
				}
			}
		}(peer)
	}
}

// --- RequestVote RPC handler -----------------------------------------

func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	if args.Term < n.currentTerm {
		return reply // stale candidate
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	reply.Term = n.currentTerm

	lastIdx, lastTerm := n.lastLogIndexAndTerm()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	if canVote && logOK {
		n.votedFor = args.CandidateID
		_ = n.persister.SaveState(PersistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor})
		reply.VoteGranted = true
		n.resetElectionTimer()
	}
	return reply
}

// --- becoming leader / follower (caller must hold n.mu) ---------------

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.id
	n.nextIndex = make(map[string]int)
	n.matchIndex = make(map[string]int)
	n.inFlight = make(map[string]bool)
	lastIdx := len(n.log) - 1
	for _, p := range n.peerAddrs {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}
	n.logger("node %s became LEADER for term %d", n.id, n.currentTerm)

	// Commit a no-op entry immediately: per the Raft paper (section 5.4.2)
	// a leader cannot conclude an entry from a *previous* term is committed
	// just by counting replicas, so it appends a no-op in its own term
	// first thing, which also lets clients started before the entry
	// eventually see progress.
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Index: lastIdx + 1, Command: Command{Op: OpNoop}})
	_ = n.persister.AppendEntries(n.log[len(n.log)-1:])

	term := n.currentTerm
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.leaderLoop(term)
	}()
}

func (n *Node) becomeFollowerLocked(term int) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	_ = n.persister.SaveState(PersistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor})
}

// --- leader replication loop -------------------------------------------

// leaderLoop sends periodic heartbeats/replication until this node steps
// down or a newer term is discovered. `term` pins the loop to the term
// in which it was elected so a stale leaderLoop from a previous election
// can't clobber state after a re-election.
func (n *Node) leaderLoop(term int) {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for {
		n.mu.Lock()
		if n.stopped || n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		for _, peer := range n.peerAddrs {
			if peer == n.id {
				continue
			}
			n.mu.Lock()
			if n.inFlight[peer] {
				n.mu.Unlock()
				continue // previous RPC to this peer still outstanding; skip this tick
			}
			n.inFlight[peer] = true
			n.mu.Unlock()
			go n.replicateTo(peer, term)
		}

		select {
		case <-n.stopc:
			return
		case <-ticker.C:
		}
	}
}

// replicateTo sends one AppendEntries RPC to `peer`, containing whatever
// entries it is currently missing, and advances commitIndex on success.
// Always clears the inFlight flag on exit so the next heartbeat tick can
// send again.
func (n *Node) replicateTo(peer string, term int) {
	defer func() {
		n.mu.Lock()
		n.inFlight[peer] = false
		n.mu.Unlock()
	}()

	n.mu.Lock()
	if n.stopped || n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	ni := n.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevIdx := ni - 1
	prevTerm := 0
	if prevIdx >= 0 && prevIdx < len(n.log) {
		prevTerm = n.log[prevIdx].Term
	}
	var entries []LogEntry
	if ni < len(n.log) {
		entries = append(entries, n.log[ni:]...)
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.transport.SendAppendEntries(peer, args)
	if err != nil || reply == nil {
		return // peer unreachable this round; next heartbeat will retry
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := prevIdx + len(entries)
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = newMatch + 1
		n.advanceCommitIndexLocked()
		return
	}

	// Conflict: back off nextIndex using the fast-backtrack hint.
	if reply.ConflictTerm != 0 {
		idx := -1
		for i := len(n.log) - 1; i >= 0; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				idx = i
				break
			}
		}
		if idx >= 0 {
			n.nextIndex[peer] = idx + 1
		} else {
			n.nextIndex[peer] = reply.ConflictIndex
		}
	} else {
		n.nextIndex[peer] = reply.ConflictIndex
	}
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}
}

// advanceCommitIndexLocked implements the majority-match rule: commitIndex
// may advance to N if a majority of matchIndex[peer] >= N AND
// log[N].Term == currentTerm (the term restriction from section 5.4.2,
// which is exactly why becomeLeaderLocked appends a no-op immediately).
func (n *Node) advanceCommitIndexLocked() {
	for nIdx := len(n.log) - 1; nIdx > n.commitIndex; nIdx-- {
		if n.log[nIdx].Term != n.currentTerm {
			continue
		}
		count := 1 // self
		for _, p := range n.peerAddrs {
			if p == n.id {
				continue
			}
			if n.matchIndex[p] >= nIdx {
				count++
			}
		}
		if count >= len(n.peerAddrs)/2+1 {
			n.commitIndex = nIdx
			n.logger("node %s advanced commitIndex to %d (term %d)", n.id, nIdx, n.currentTerm)
			break
		}
	}
}

// --- AppendEntries RPC handler -----------------------------------------

func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{Term: n.currentTerm, Success: false}

	if args.Term < n.currentTerm {
		return reply // stale leader
	}
	if args.Term > n.currentTerm || n.state == Candidate {
		n.becomeFollowerLocked(args.Term)
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	// Consistency check on PrevLogIndex/PrevLogTerm.
	if args.PrevLogIndex >= len(n.log) {
		reply.ConflictIndex = len(n.log)
		reply.ConflictTerm = 0
		reply.LogLen = len(n.log)
		return reply
	}
	if args.PrevLogIndex >= 0 && n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex].Term
		firstIdx := args.PrevLogIndex
		for firstIdx > 0 && n.log[firstIdx-1].Term == conflictTerm {
			firstIdx--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = firstIdx
		return reply
	}

	// Append/overwrite entries, truncating on first conflict.
	insertAt := args.PrevLogIndex + 1
	conflictAt := -1
	for i, e := range args.Entries {
		idx := insertAt + i
		if idx < len(n.log) {
			if n.log[idx].Term != e.Term {
				conflictAt = idx
				break
			}
		} else {
			conflictAt = idx
			break
		}
	}
	if conflictAt != -1 {
		keep := n.log[:conflictAt]
		newTail := args.Entries[conflictAt-insertAt:]
		n.log = append(append([]LogEntry{}, keep...), newTail...)
		_ = n.persister.TruncateAndReplace(n.log)
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
	}

	reply.Success = true
	reply.LogLen = len(n.log)
	return reply
}

// --- client-facing API --------------------------------------------------

// Start appends a new command to the leader's log and begins replicating
// it. Returns the index/term it was placed at and whether this node is
// (believed to be) the leader; the caller must wait for that index to be
// committed (e.g. by watching the apply channel) before it is durable.
func (n *Node) Start(cmd Command) (index int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return -1, n.currentTerm, false
	}
	index = len(n.log)
	entry := LogEntry{Term: n.currentTerm, Index: index, Command: cmd}
	n.log = append(n.log, entry)
	_ = n.persister.AppendEntries([]LogEntry{entry})
	return index, n.currentTerm, true
}

func (n *Node) Status() (id string, state State, term int, leaderID string, commitIndex int, logLen int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.id, n.state, n.currentTerm, n.leaderID, n.commitIndex, len(n.log)
}

func (n *Node) lastLogIndexAndTerm() (int, int) {
	last := len(n.log) - 1
	return last, n.log[last].Term
}

// applyLoop pushes newly committed entries to applyCh in order. This is
// what the KV state machine listens on.
func (n *Node) applyLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopc:
			return
		case <-ticker.C:
			n.mu.Lock()
			var toApply []ApplyMsg
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				e := n.log[n.lastApplied]
				toApply = append(toApply, ApplyMsg{
					CommandValid: true,
					Command:      e.Command,
					CommandIndex: e.Index,
					CommandTerm:  e.Term,
				})
			}
			n.mu.Unlock()
			for _, msg := range toApply {
				select {
				case n.applyCh <- msg:
				case <-n.stopc:
					return
				}
			}
		}
	}
}
