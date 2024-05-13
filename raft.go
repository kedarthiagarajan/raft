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
	"fmt"
	"math/rand"
	"raft/labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
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
	Term    int
	Success bool
}

//
// A Go object implementing a single Raft peer.
//

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

type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	timerMu   sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh         chan ApplyMsg // Channel for the commit to the state machine
	heartbeatCh     chan AppendEntriesRequest
	voteCh          chan RequestVoteReply
	electionStartCh chan struct{}

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    *int
	log         []LogEntry
	voteCount   int

	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// RaftState represents the current state of a Raft peer.
	state RaftState

	// Timer state variables to track timeout and last reset time
	electionTimeoutDuration time.Duration
	lastResetTime           time.Time
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("Server %d did not give the vote to server %d\n", rf.me, args.CandidateId)
		return
	} else {
		if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
			fmt.Printf("Server %d granted the vote to server %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
			rf.currentTerm = args.Term
			rf.heartbeatCh <- AppendEntriesRequest{} // count this as a heartbeat
			// rf.safeUpdate("lastResetTime", time.Now())
			// Persist state change
			rf.persist()
		} else {
			reply.VoteGranted = false
		}
	}
	// Your code here (3, 4).
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < currTerm {
		rf.mu.Lock()
		reply.Term = currTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	currTerm = rf.currentTerm
	rf.mu.Unlock()
	if args.Term > currTerm {
		fmt.Printf("Server %d received a heartbeat with a higher term than ours\n", rf.me)
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = nil

		// rf.safeUpdate("currentTerm", args.Term)
		// rf.safeUpdate("state", Follower)
		// rf.safeUpdate("votedFor", nil)
		rf.persist()
		rf.mu.Unlock()
	}
	// rf.resetTimer(args.LeaderId)

	rf.heartbeatCh <- *args
	// 1. Reply false if term < currentTerm (§5.1)
	// if args.Term < rf.currentTerm {
	// 	reply.Success = false
	// 	reply.Term = rf.currentTerm
	// 	return
	// }

	// // 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// // whose term matches prevLogTerm (§5.3)
	// if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	reply.Success = false
	// 	reply.Term = rf.currentTerm
	// 	return
	// }

	// // 3. If an existing entry conflicts with a new one (same index
	// // but different terms), delete the existing entry and all that
	// // follow it (§5.3)
	// for i, entry := range args.Entries {
	// 	if entry.Index <= len(rf.log)-1 && rf.log[entry.Index].Term != entry.Term {
	// 		rf.log = rf.log[:entry.Index]
	// 		break
	// 	}
	// }

	// // 4. Append any new entries not already in the log
	// if len(args.Entries) > 0 {
	// 	rf.log = append(rf.log, args.Entries...)
	// }

	// // 5. If leaderCommit > commitIndex, set commitIndex =
	// // min(leaderCommit, index of last new entry)
	// if args.LeaderCommit > rf.commitIndex {
	// 	lastNewEntryIndex := len(rf.log) - 1
	// 	if lastNewEntryIndex < args.LeaderCommit {
	// 		rf.commitIndex = lastNewEntryIndex
	// 	} else {
	// 		rf.commitIndex = args.LeaderCommit
	// 	}
	// 	// Apply newly committed entries
	// 	rf.applyCommittedEntries()
	// }

	// reply.Success = true
	// reply.Term = rf.currentTerm
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
	rf.heartbeatCh = make(chan AppendEntriesRequest)
	rf.applyCh = make(chan ApplyMsg)
	rf.voteCh = make(chan RequestVoteReply)
	rf.electionStartCh = make(chan struct{}, 1)
}

func (rf *Raft) switchToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = nil

}

func (rf *Raft) switchToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = &rf.me
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
	term := 0
	isLeader := false

	// Your code here (4).

	return index, term, isLeader
}

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
	fmt.Printf("Server %d ", rf.me)
	switch name {
	case "currentTerm":
		fmt.Printf("Read currentTerm: %d\n", rf.currentTerm)
		return rf.currentTerm
	case "votedFor":
		if rf.votedFor != nil {
			fmt.Printf("Read votedFor: %d\n", *rf.votedFor)
			return *rf.votedFor
		} else {
			fmt.Printf("Read votedFor: nil\n")
			return nil
		}
	case "voteCount":
		fmt.Printf("Read server %d voteCount: %d\n", rf.me, rf.voteCount)
		return rf.voteCount
	case "state":
		fmt.Printf("Read server %d state: %s\n", rf.me, rf.state.String())
		return rf.state
	case "log":
		fmt.Printf("Read log: %+v\n", rf.log)
		return rf.log
	case "commitIndex":
		fmt.Printf("Read commitIndex: %d\n", rf.commitIndex)
		return rf.commitIndex
	case "lastApplied":
		fmt.Printf("Read lastApplied: %d\n", rf.lastApplied)
		return rf.lastApplied
	case "nextIndex":
		fmt.Printf("Read nextIndex: %+v\n", rf.nextIndex)
		return rf.nextIndex
	case "matchIndex":
		fmt.Printf("Read matchIndex: %+v\n", rf.matchIndex)
		return rf.matchIndex
	case "electionTimeoutDuration":
		fmt.Printf("Read electionTimeoutDuration: %v\n", rf.electionTimeoutDuration)
		return rf.electionTimeoutDuration
	case "lastResetTime":
		fmt.Printf("Read lastResetTime: %v\n", rf.lastResetTime)
		return rf.lastResetTime
	default:
		fmt.Printf("safeRead: unknown field name %s\n", name)
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
			fmt.Printf("Server %d Updated currentTerm to %d\n", rf.me, v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for currentTerm\n")
		}
	case "votedFor":
		if v, ok := value.(*int); ok {
			rf.votedFor = v
			fmt.Printf("Server %d Updated votedFor to %d\n", rf.me, v)
		} else if value == nil {
			rf.votedFor = nil
			fmt.Printf("Updated votedFor to nil\n")
		} else {
			fmt.Printf("safeUpdate: type assertion failed for votedFor\n")
		}
	case "voteCount":
		if v, ok := value.(int); ok {
			rf.voteCount = v
			fmt.Printf("Server %d Updated votedFor to %d\n", rf.me, v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for votedCount\n")
		}
	case "state":
		if v, ok := value.(RaftState); ok {
			rf.state = v
			fmt.Printf("Server %d Updated state to %v\n", rf.me, v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for state\n")
		}
	case "log":
		if v, ok := value.([]LogEntry); ok {
			rf.log = v
			fmt.Printf("Updated log to %+v\n", v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for log\n")
		}
	case "commitIndex":
		if v, ok := value.(int); ok {
			rf.commitIndex = v
			fmt.Printf("Updated commitIndex to %d\n", v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for commitIndex\n")
		}
	case "lastApplied":
		if v, ok := value.(int); ok {
			rf.lastApplied = v
			fmt.Printf("Updated lastApplied to %d\n", v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for lastApplied\n")
		}
	case "nextIndex":
		if v, ok := value.([]int); ok {
			rf.nextIndex = v
			fmt.Printf("Updated nextIndex to %+v\n", v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for nextIndex\n")
		}
	case "matchIndex":
		if v, ok := value.([]int); ok {
			rf.matchIndex = v
			fmt.Printf("Updated matchIndex to %+v\n", v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for matchIndex\n")
		}
	case "electionTimeoutDuration":
		if v, ok := value.(time.Duration); ok {
			rf.electionTimeoutDuration = v
			fmt.Printf("Updated electionTimeoutDuration to %v\n", v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for electionTimeoutDuration\n")
		}
	case "lastResetTime":
		if v, ok := value.(time.Time); ok {
			rf.lastResetTime = v
			fmt.Printf("Updated lastResetTime to %v\n", v)
		} else {
			fmt.Printf("safeUpdate: type assertion failed for lastResetTime\n")
		}
	default:
		fmt.Printf("safeUpdate: unknown field name %s\n", name)
	}
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var reply AppendEntriesReply
				rf.mu.Lock()
				args := AppendEntriesRequest{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
					Entries:      nil, // heartbeat contains no entries
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				fmt.Printf("Server %d sent heartbeat to Server %d\n", rf.me, server)
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.mu.Lock()
					currT := rf.currentTerm
					rf.mu.Unlock()
					if reply.Term > currT {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = nil

						// rf.safeUpdate("currentTerm", reply.Term)
						// rf.safeUpdate("state", Follower)
						// rf.safeUpdate("votedFor", nil)
						rf.persist()
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) run() {
	for {
		// rf.mu.Lock()
		// print some stupid message that we started

		// elapsedTime := currentTime.Sub(rf.lastResetTime)
		// fmt.Printf("Elapsed time: %v, Reset timer: %v\n", elapsedTime, rf.electionTimeoutDuration)
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Follower:
			select {
			case <-rf.heartbeatCh:
				fmt.Printf("Server %d received a heartbeat\n", rf.me)
			case <-time.After(rf.electionTimeoutDuration):
				rf.mu.Lock()
				rf.switchToCandidate()
				rf.mu.Unlock()
			}
			// rf.mu.Unlock()
			// // fmt.Printf("Server %d: follower\n", rf.me)
			// rf.timerMu.Lock()
			// currentTimeMillis := time.Now().UnixNano() / int64(time.Millisecond)
			// lastResetTimeMillis := rf.lastResetTime.UnixNano() / int64(time.Millisecond)
			// timeDiffMillis := currentTimeMillis - lastResetTimeMillis
			// if time.Duration(timeDiffMillis)*time.Millisecond > rf.electionTimeoutDuration {
			// 	fmt.Printf("Server %d: Becoming a candidate at %d, election timeout: %v, last reset time: %d, time diff: %dms\n", rf.me, currentTimeMillis, rf.electionTimeoutDuration, lastResetTimeMillis, timeDiffMillis)
			// 	rf.state = Candidate
			// }
			// rf.timerMu.Unlock()
			// rf.mu.Unlock()
		case Candidate:
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: -1,
				LastLogTerm:  -1,
			}
			rf.mu.Unlock()

			select {
			case <-rf.electionStartCh:
				fmt.Printf("Server %d is starting an election\n", rf.me)
				for i := range rf.peers {
					if i != rf.me {
						go func(server int) {
							fmt.Printf("Server %d is sending a vote request to server %d\n", rf.me, server)
							var reply RequestVoteReply
							if rf.sendRequestVote(server, &args, &reply) {
								if reply.VoteGranted {
									fmt.Printf("vote granted\n")
									rf.voteCh <- reply
									fmt.Printf("sent vote to channel\n")
								}
							}
						}(i)
					}
				}
			case reply := <-rf.voteCh:
				rf.mu.Lock()
				fmt.Printf("Processing vote\n")
				if rf.state != Candidate {
					fmt.Print("Race condition, we reset to a different state")
					return
				}

				if reply.Term > rf.currentTerm {
					fmt.Printf("Server %d found higher term %d in reply\n", rf.me, reply.Term)
					rf.switchToFollower(rf.currentTerm)
					rf.persist()
				} else if reply.VoteGranted && rf.state == Candidate {
					rf.voteCount++
					fmt.Printf("Server %d received vote\n", rf.me)
					if rf.voteCount*2 > len(rf.peers) {
						fmt.Printf("Server %d has become the leader with %d votes\n", rf.me, rf.voteCount)
						rf.switchToLeader()
						go rf.sendHeartbeat()
					}
				}
				rf.mu.Unlock()
			case <-time.After(rf.electionTimeoutDuration):
				fmt.Printf("Server %d election timeout, failed to win the election with %d votes\n", rf.me, rf.voteCount)
				rf.safeUpdate("state", Follower)
				rf.safeUpdate("votedFor", nil)
				rf.mu.Lock()
				rf.electionTimeoutDuration = time.Duration(150+rand.Intn(151)) * time.Millisecond
				rf.mu.Unlock()
			}

		case Leader:
			fmt.Printf("Server %d is the leader\n", rf.me)
			select {
			case <-time.After(100 * time.Millisecond):
				go rf.sendHeartbeat()
			}

		}
	}
	// Implementation for the main function goes here.
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{electionTimeoutDuration: time.Duration(150+rand.Intn(151)) * time.Millisecond,
		lastResetTime: time.Now()}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.heartbeatCh = make(chan AppendEntriesRequest)
	rf.applyCh = make(chan ApplyMsg)
	rf.voteCh = make(chan RequestVoteReply)
	rf.electionStartCh = make(chan struct{}, 1)
	rf.currentTerm = 0
	// Your initialization code here (3, 4).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.run()

	return rf
}
