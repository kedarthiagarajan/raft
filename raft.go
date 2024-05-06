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

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    *int
	log         []LogEntry

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else {
		if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
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
	defer rf.mu.Unlock()
	fmt.Printf("Server with ID %d has reset its timer\n", rf.me)
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = nil
		rf.persist()
	}
	rf.lastResetTime = time.Now()
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
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

func (rf *Raft) run() {
	for {
		rf.mu.Lock()
		// print some stupid message that we started
		currentTime := time.Now()
		// elapsedTime := currentTime.Sub(rf.lastResetTime)
		// fmt.Printf("Elapsed time: %v, Reset timer: %v\n", elapsedTime, rf.electionTimeoutDuration)
		if rf.state != Leader && currentTime.Sub(rf.lastResetTime) > rf.electionTimeoutDuration {
			fmt.Printf("Server %d: Not the leader\n", rf.me)
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = &rf.me
			rf.lastResetTime = currentTime
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: -1,
				LastLogTerm:  -1,
			}
			rf.mu.Unlock()

			votesReceived := 1 // We start with 1 because a candidate votes for itself
			for i := range rf.peers {
				if i != rf.me {
					fmt.Printf("Peer %d is sending a vote request to peer %d\n", rf.me, i)
					go func(server int) {
						fmt.Println("sending")
						var reply RequestVoteReply
						if rf.sendRequestVote(server, &args, &reply) {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							// Check if the term in the reply is greater than our current term
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.votedFor = nil
								rf.persist()
							} else if reply.VoteGranted && rf.state == Candidate {
								votesReceived++
								// If we've received the majority of votes, become the leader
								if votesReceived*2 > len(rf.peers) {
									rf.state = Leader
									// Reinitialize nextIndex and matchIndex for all followers
									rf.nextIndex = make([]int, len(rf.peers))
									rf.matchIndex = make([]int, len(rf.peers))
									for i := range rf.nextIndex {
										rf.nextIndex[i] = len(rf.log)
									}
									// Send initial empty AppendEntries RPCs (heartbeat) to each server
									// This is done to establish authority as leader
									// (code for sending heartbeats is not shown here)
								}
							}
						}
					}(i)
				}
			}
		} else {
			rf.mu.Unlock()
		}

		if rf.state == Leader {
			fmt.Printf("Server %d is the leader\n", rf.me)
			ticker := time.NewTicker(100 * time.Millisecond) // 10 times a second
			go func() {
				for {
					select {
					case <-ticker.C:
						rf.mu.Lock()
						for i := range rf.peers {
							if i != rf.me {
								go func(server int) {
									var reply AppendEntriesReply
									args := AppendEntriesRequest{
										Term:         rf.currentTerm,
										LeaderId:     rf.me,
										PrevLogIndex: -1,
										PrevLogTerm:  -1,
										Entries:      nil, // heartbeat contains no entries
										LeaderCommit: rf.commitIndex,
									}
									if rf.sendAppendEntries(server, &args, &reply) {
										rf.mu.Lock()
										defer rf.mu.Unlock()
										if reply.Term > rf.currentTerm {
											rf.currentTerm = reply.Term
											rf.state = Follower
											rf.votedFor = nil
											rf.persist()
										}
									}
								}(i)
							}
						}
						rf.mu.Unlock()
					}
				}
			}()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
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

	// Your initialization code here (3, 4).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.run()

	return rf
}
