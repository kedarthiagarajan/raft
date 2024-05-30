package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"bytes"
	"log"
	"math/rand"
	"raft/labgob"
	"raft/labrpc"
	"sync"
	"time"
)

// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (state RaftState) String() string {
	switch state {
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

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term               int
	MismatchedTerm     int
	MismatchStartIndex int
	Success            bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	timerMu   sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // Channel for the commit to the state machine
	//heartbeatCh     chan AppendEntriesRequest
	voteCh          chan RequestVoteReply
	electionStartCh chan struct{}

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	voteCount   int

	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// RaftState represents the current state of a Raft peer.
	state    RaftState
	leaderId int

	electionTimeoutDuration time.Duration
	lastResetTime           time.Time

	applyCondition     *sync.Cond
	leaderCondition    *sync.Cond
	nonLeaderCondition *sync.Cond

	lastHeartbeatSentTs int64
	lastHeartbeatRecvTs int64

	electionTimeout   int
	heartbeatInterval int

	electionTimeoutCh chan bool
	heartbeatPeriodCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()

	// Your code here (3).

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry

	decodeError := d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil
	if decodeError {
		log.Fatal("decode error in read persist!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (3, 4).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3).
}

func (rf *Raft) resetTimer(id int) {
	rf.timerMu.Lock()
	// oldResetTime := rf.lastResetTime.UnixNano() / int64(time.Millisecond)
	rf.lastResetTime = time.Now()
	// newResetTime := rf.lastResetTime.UnixNano() / int64(time.Millisecond)
	rf.timerMu.Unlock()
	// fmt.Printf("Server with ID %d has reset its timer from %dms to %dms due to a message from Server %d\n", rf.me, oldResetTime, newResetTime, id)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.changeState(Follower)
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastIndex := len(rf.log) - 1
		lastTerm := rf.log[lastIndex].Term

		if lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex) {

			rf.votedFor = args.CandidateId
			rf.resetElectionTimer()
			rf.persist()

			reply.VoteGranted = true
			return
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(Follower)
		rf.resetElectionTimer()
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		return
	}

	rf.resetElectionTimer()
	rf.changeState(Follower)

	nextIdx := args.PrevLogIndex + 1
	if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		isMatch := true
		endIdx := len(rf.log) - 1
		for i := 0; isMatch && i < len(args.Entries); i++ {
			if endIdx < nextIdx+i || rf.log[nextIdx+i].Term != args.Entries[i].Term {
				isMatch = false
			}
		}

		if !isMatch {
			newEntries := make([]LogEntry, len(args.Entries))
			copy(newEntries, args.Entries)
			rf.log = append(rf.log[:nextIdx], newEntries...)
		}

		lastNewEntryIdx := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, lastNewEntryIdx)
			rf.applyCondition.Broadcast()
		}

		rf.leaderId = args.LeaderId
		rf.persist()

		reply.Success = true
		return
	}

	if len(rf.log) < nextIdx {
		reply.MismatchedTerm = 0
		reply.MismatchStartIndex = len(rf.log)
	} else {
		reply.MismatchedTerm = rf.log[args.PrevLogIndex].Term
		reply.MismatchStartIndex = args.PrevLogIndex
		for i := reply.MismatchStartIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != reply.MismatchedTerm {
				break
			}
			reply.MismatchStartIndex -= 1
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) applyCommittedEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) clearChannels() {
	//rf.heartbeatCh = make(chan AppendEntriesRequest)
	rf.applyCh = make(chan ApplyMsg)
	rf.voteCh = make(chan RequestVoteReply)
	rf.electionStartCh = make(chan struct{}, 1)
}

func (rf *Raft) determineNextIndex(reply AppendEntriesReply, nextIdx int) int {
	if reply.MismatchedTerm == 0 {
		return reply.MismatchStartIndex
	}

	conflictIdx := reply.MismatchStartIndex
	conflictTerm := rf.log[conflictIdx].Term

	if conflictTerm < reply.MismatchedTerm {
		return reply.MismatchStartIndex
	}

	conflictIdx = rf.findConflictIndex(conflictIdx, reply.MismatchedTerm)
	if conflictIdx == 0 {
		return reply.MismatchStartIndex
	}

	conflictIdx = rf.advanceConflictIndex(conflictIdx, nextIdx, reply.MismatchedTerm)
	return conflictIdx + 1
}

func (rf *Raft) findConflictIndex(conflictIdx int, mismatchedTerm int) int {
	for i := conflictIdx; i > 0; i-- {
		if rf.log[i].Term == mismatchedTerm {
			return conflictIdx
		}
		conflictIdx -= 1
	}
	return conflictIdx
}

func (rf *Raft) advanceConflictIndex(conflictIdx int, nextIdx int, mismatchedTerm int) int {
	for i := conflictIdx + 1; i < nextIdx; i++ {
		if rf.log[i].Term != mismatchedTerm {
			break
		}
		conflictIdx += 1
	}
	return conflictIdx
}

func (rf *Raft) sendAppendEntriesToAll(logIdx, term, commitIdx, replicaCount int, taskName string) {
	var wg sync.WaitGroup
	majority := len(rf.peers)/2 + 1
	consensus := false

	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()
	if rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)

		go rf.handleAppendEntries(i, logIdx, term, commitIdx, majority, &wg, &replicaCount, &consensus)
	}
	wg.Wait()
}

func (rf *Raft) handleAppendEntries(peerIdx, logIdx, term, commitIdx, majority int, wg *sync.WaitGroup, replicaCount *int, consensus *bool) {
	defer wg.Done()

	for {
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}

		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		nextIdx := rf.nextIndex[peerIdx]
		prevLogIdx := nextIdx - 1
		prevLogTerm := rf.log[prevLogIdx].Term

		entries := make([]LogEntry, 0)
		if nextIdx < logIdx+1 {
			entries = rf.log[nextIdx : logIdx+1]
		}
		args := AppendEntriesRequest{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIdx,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIdx,
		}
		rf.mu.Unlock()

		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peerIdx, &args, &reply)

		if !ok {
			return
		}

		rf.mu.Lock()
		if rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if !reply.Success {
			if args.Term < reply.Term {
				rf.updateTerm(reply.Term)
				rf.mu.Unlock()
				return
			} else {
				nextIdx := rf.determineNextIndex(reply, nextIdx)
				rf.nextIndex[peerIdx] = nextIdx
				rf.mu.Unlock()
				continue
			}
		}

		rf.updateIndexesAfterSuccess(peerIdx, logIdx, majority, replicaCount, consensus)
		rf.mu.Unlock()
		break
	}
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.changeState(Follower)
	rf.persist()
}

func (rf *Raft) updateIndexesAfterSuccess(peerIdx, logIdx, majority int, replicaCount *int, consensus *bool) {
	if rf.nextIndex[peerIdx] < logIdx+1 {
		rf.nextIndex[peerIdx] = logIdx + 1
		rf.matchIndex[peerIdx] = logIdx
	}
	*replicaCount++
	if rf.state == Leader && *replicaCount >= majority && !*consensus {
		*consensus = true
		if rf.commitIndex < logIdx && rf.log[logIdx].Term == rf.currentTerm {
			rf.commitIndex = logIdx
			go rf.broadcastHeartbeat()
			rf.applyCondition.Broadcast()
		}
	}
}

func (rf *Raft) switchToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1

}

func (rf *Raft) switchToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionStartCh <- struct{}{}

}

func (rf *Raft) switchToLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if thfse Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0
	term, isLeader := rf.GetState()

	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()

	// Create a new log entry
	logEntry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, logEntry)
	index = len(rf.log) - 1
	rf.lastHeartbeatSentTs = time.Now().UnixNano()
	rf.persist()
	replicaCount := 1

	rf.mu.Unlock()

	// Start sending append entries to all peers
	go rf.sendAppendEntriesToAll(index, term, rf.commitIndex, replicaCount, "Start")

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) safeRead(name string) interface{} {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("Server %d ", rf.me)
	switch name {
	case "currentTerm":
		// fmt.Printf("Read currentTerm: %d\n", rf.currentTerm)
		return rf.currentTerm
	case "votedFor":
		if rf.votedFor != -1 {
			// fmt.Printf("Read votedFor: %d\n", *rf.votedFor)
			return rf.votedFor
		} else {
			// fmt.Printf("Read votedFor: nil\n")
			return nil
		}
	case "voteCount":
		// fmt.Printf("Read server %d voteCount: %d\n", rf.me, rf.voteCount)
		return rf.voteCount
	case "state":
		// fmt.Printf("Read server %d state: %s\n", rf.me, rf.state.String())
		return rf.state
	case "log":
		// fmt.Printf("Read log: %+v\n", rf.log)
		return rf.log
	case "commitIndex":
		// fmt.Printf("Read commitIndex: %d\n", rf.commitIndex)
		return rf.commitIndex
	case "lastApplied":
		// fmt.Printf("Read lastApplied: %d\n", rf.lastApplied)
		return rf.lastApplied
	case "nextIndex":
		// fmt.Printf("Read nextIndex: %+v\n", rf.nextIndex)
		return rf.nextIndex
	case "matchIndex":
		// fmt.Printf("Read matchIndex: %+v\n", rf.matchIndex)
		return rf.matchIndex
	case "electionTimeoutDuration":
		// fmt.Printf("Read electionTimeoutDuration: %v\n", rf.electionTimeoutDuration)
		return rf.electionTimeoutDuration
	case "lastResetTime":
		// fmt.Printf("Read lastResetTime: %v\n", rf.lastResetTime)
		return rf.lastResetTime
	default:
		// fmt.Printf("safeRead: unknown field name %s\n", name)
		return nil
	}

}

func (rf *Raft) safeUpdate(name string, value interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch name {
	case "currentTerm":
		if v, ok := value.(int); ok {
			rf.currentTerm = v
			// fmt.Printf("Server %d Updated currentTerm to %d\n", rf.me, v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for currentTerm\n")
		}
	case "votedFor":
		if v, ok := value.(int); ok {
			rf.votedFor = v
			// fmt.Printf("Server %d Updated votedFor to %d\n", rf.me, v)
		} else if value == nil {
			rf.votedFor = -1
			// fmt.Printf("Updated votedFor to nil\n")
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for votedFor\n")
		}
	case "voteCount":
		if v, ok := value.(int); ok {
			rf.voteCount = v
			// fmt.Printf("Server %d Updated votedFor to %d\n", rf.me, v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for votedCount\n")
		}
	case "state":
		if v, ok := value.(RaftState); ok {
			rf.state = v
			// fmt.Printf("Server %d Updated state to %v\n", rf.me, v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for state\n")
		}
	case "log":
		if v, ok := value.([]LogEntry); ok {
			rf.log = v
			// fmt.Printf("Updated log to %+v\n", v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for log\n")
		}
	case "commitIndex":
		if v, ok := value.(int); ok {
			rf.commitIndex = v
			// fmt.Printf("Updated commitIndex to %d\n", v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for commitIndex\n")
		}
	case "lastApplied":
		if v, ok := value.(int); ok {
			rf.lastApplied = v
			// fmt.Printf("Updated lastApplied to %d\n", v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for lastApplied\n")
		}
	case "nextIndex":
		if v, ok := value.([]int); ok {
			rf.nextIndex = v
			// fmt.Printf("Updated nextIndex to %+v\n", v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for nextIndex\n")
		}
	case "matchIndex":
		if v, ok := value.([]int); ok {
			rf.matchIndex = v
			// fmt.Printf("Updated matchIndex to %+v\n", v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for matchIndex\n")
		}
	case "electionTimeoutDuration":
		if v, ok := value.(time.Duration); ok {
			rf.electionTimeoutDuration = v
			// fmt.Printf("Updated electionTimeoutDuration to %v\n", v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for electionTimeoutDuration\n")
		}
	case "lastResetTime":
		if v, ok := value.(time.Time); ok {
			rf.lastResetTime = v
			// fmt.Printf("Updated lastResetTime to %v\n", v)
		} else {
			// fmt.Printf("safeUpdate: type assertion failed for lastResetTime\n")
		}
	default:
		// fmt.Printf("safeUpdate: unknown field name %s\n", name)
	}
}

func (rf *Raft) changeState(newState RaftState) {
	oldState := rf.state
	rf.state = newState
	if oldState == Leader && newState == Follower {
		rf.nonLeaderCondition.Broadcast()
	} else if oldState == Candidate && newState == Leader {
		rf.leaderCondition.Broadcast()
	}
}

func (rf *Raft) processCommittedEntries() {
	for {
		rf.mu.Lock()
		commitIdx := rf.commitIndex
		lastAppliedIdx := rf.lastApplied
		rf.mu.Unlock()

		if lastAppliedIdx == commitIdx {
			rf.mu.Lock()
			rf.applyCondition.Wait()
			rf.mu.Unlock()
		} else {
			for i := lastAppliedIdx + 1; i <= commitIdx; i++ {
				rf.mu.Lock()
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
				rf.lastApplied = i
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
			}
		}
	}
}

// Checks for timeout events for election and heartbeat.
func (rf *Raft) timeoutChecker() {
	for {
		rf.mu.Lock()
		elapseTime := time.Now().UnixNano()
		if rf.state == Leader {
			if int64(elapseTime-rf.lastHeartbeatSentTs)/int64(time.Millisecond) >= int64(rf.heartbeatInterval) {
				rf.heartbeatPeriodCh <- true
			}
		} else {
			if int64(elapseTime-rf.lastHeartbeatRecvTs)/int64(time.Millisecond) >= int64(rf.electionTimeout) {
				rf.electionTimeoutCh <- true
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

// Handles timeout events for both election and heartbeat periods.
func (rf *Raft) handleTimeouts() {
	for {
		select {
		case <-rf.electionTimeoutCh:
			rf.mu.Lock()
			go rf.run()
			rf.mu.Unlock()
		case <-rf.heartbeatPeriodCh:
			rf.mu.Lock()
			go rf.sendHeartbeat()
			rf.mu.Unlock()
		}
	}
}

// Sends heartbeat to all peers and ensures consistency.
func (rf *Raft) sendHeartbeat() {
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()
	rf.lastHeartbeatSentTs = time.Now().UnixNano()
	index := len(rf.log) - 1
	nReplica := 1
	rf.mu.Unlock()

	go rf.sendAppendEntriesToAll(index, rf.currentTerm, rf.commitIndex, nReplica, "Broadcast")
}

func (rf *Raft) broadcastHeartbeat() {
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()
	rf.lastHeartbeatSentTs = time.Now().UnixNano()
	rf.mu.Unlock()

	rf.mu.Lock()
	index := len(rf.log) - 1
	replicaCount := 1
	go rf.sendAppendEntriesToAll(index, rf.currentTerm, rf.commitIndex, replicaCount, "Heartbeat")
	rf.mu.Unlock()
}

// Resets the election timer with a new randomized timeout value.
func (rf *Raft) resetElectionTimer() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rf.heartbeatInterval*5 + rand.Intn(150)
	rf.lastHeartbeatRecvTs = time.Now().UnixNano()
}

func (rf *Raft) run() {
	// Start the election process by transitioning to Candidate state
	rf.mu.Lock()
	rf.changeState(Candidate)
	rf.currentTerm++
	rf.votedFor = rf.me
	voteCounter := 1
	rf.resetElectionTimer()
	rf.persist()
	rf.mu.Unlock()

	// Start the voting process in a separate goroutine
	go rf.startElection(voteCounter)
}

func (rf *Raft) startElection(voteCounter int) {
	var wg sync.WaitGroup
	majority := len(rf.peers)/2 + 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go rf.requestVoteFromPeer(i, &voteCounter, &wg, majority)
	}

	// Wait for all RPCs to complete
	wg.Wait()
}

func (rf *Raft) requestVoteFromPeer(peerIdx int, voteCounter *int, wg *sync.WaitGroup, majority int) {
	defer wg.Done()

	rf.mu.Lock()
	lastLogIndex := len(rf.log) - 1
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	rf.mu.Unlock()

	var voteReply RequestVoteReply
	ok := rf.sendRequestVote(peerIdx, &voteArgs, &voteReply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != voteArgs.Term {
		return
	}

	if voteReply.VoteGranted {
		*voteCounter++
		if rf.state == Candidate && *voteCounter >= majority {
			rf.changeStateToLeader()
		}
	} else if rf.currentTerm < voteReply.Term {
		rf.updateTermToFollower(voteReply.Term)
	}
}

func (rf *Raft) changeStateToLeader() {
	rf.changeState(Leader)
	rf.leaderId = rf.me

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	go rf.sendHeartbeat()
	rf.persist()
}

func (rf *Raft) updateTermToFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.changeState(Follower)
	rf.persist()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Initialization
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		applyCh:           applyCh,
		state:             Follower,
		leaderId:          -1,
		heartbeatInterval: 120, // max 100ms heartbeat period
		electionTimeoutCh: make(chan bool),
		heartbeatPeriodCh: make(chan bool),
		currentTerm:       0,
		votedFor:          -1, // -1 means no vote given
		commitIndex:       0,
		lastApplied:       0,
		log:               make([]LogEntry, 1),
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
	}

	rf.log[0] = LogEntry{Term: 0}

	rf.applyCondition = sync.NewCond(&rf.mu)
	rf.leaderCondition = sync.NewCond(&rf.mu)
	rf.nonLeaderCondition = sync.NewCond(&rf.mu)

	go rf.timeoutChecker()
	go rf.handleTimeouts()
	go rf.processCommittedEntries()

	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	return rf
}
