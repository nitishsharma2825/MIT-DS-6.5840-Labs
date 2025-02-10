package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state State

	currentLeader int

	lastPinged time.Time

	reply chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[%d] received ap with log index %d, current log len %d\n", rf.me, args.PrevLogIndex, len(rf.log))
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// args.Term >= rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.currentLeader = args.LeaderId
		rf.lastPinged = time.Now()
		rf.votedFor = -1
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex != 0 && (args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// If an existing entry conflict with a new one (same index but differnt terms), delete the existing entry and all that follows it
	if args.Entries != nil {
		currentLogEntry := args.Entries[0]
		if len(rf.log) > currentLogEntry.Index && rf.log[currentLogEntry.Index-1].Term != currentLogEntry.Term {
			rf.log = rf.log[:args.PrevLogIndex]
		}

		// Append any new entries to the log
		rf.log = append(rf.log, args.Entries...)
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
			entry := rf.log[i-1]
			reply := ApplyMsg{
				CommandValid: true,
				CommandIndex: entry.Index,
				Command:      entry.Command,
			}

			go func(reply ApplyMsg) {
				rf.reply <- reply
			}(reply)
		}
		rf.commitIndex = args.LeaderCommit
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.state = Follower
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) attemptElection() {
	// fetch all state during this critical section
	rf.mu.Lock()

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.lastPinged = time.Now()

	term := rf.currentTerm
	candidateId := rf.me

	votes := 1

	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == candidateId {
			continue
		}

		go func(server int, term int, candidateId int) {
			args := RequestVoteArgs{
				Term:        term,
				CandidateId: candidateId,
			}

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				// request timed out
				return
			}

			rf.mu.Lock()

			// check if the term has changed since the request was sent, or if the server is no longer a candidate
			if reply.Term != rf.currentTerm || rf.state != Candidate {
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votes++
			}

			if votes > len(rf.peers)/2 {
				rf.state = Leader
				rf.currentLeader = rf.me
			}

			rf.mu.Unlock()

			if rf.state == Leader {
				rf.sendHeartbeats()
			}

		}(idx, term, candidateId)
	}
}

func (rf *Raft) sendHeartbeats() {
	// fetch all required state during this critical section
	rf.mu.Lock()

	term := rf.currentTerm
	leaderId := rf.me
	commitIndex := rf.commitIndex

	prevLogIndex := 0
	prevLogTerm := 0

	logLen := len(rf.log)
	if logLen > 1 {
		prevLogEntry := rf.log[logLen-1]
		prevLogIndex = prevLogEntry.Index
		prevLogTerm = prevLogEntry.Term
	}

	defer rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		LeaderCommit: commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}

	reply := AppendEntriesReply{}

	for i := 0; i < len(rf.peers); i++ {
		if i == leaderId {
			continue
		}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}

func (rf *Raft) logReplication(logIndex int) {
	votes := 1
	// send AppendEntries RPCs to all peers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int, logIndex int) {

			logEntryToBeReplicated := rf.log[logIndex-1]
			entries := []LogEntry{logEntryToBeReplicated}
			currentTerm := rf.currentTerm
			leaderId := rf.me
			currentCommitIndex := rf.commitIndex

			prevLogIndex := 0
			prevLogTerm := 0

			if logIndex > 1 {
				prevLogEntry := rf.log[logIndex-1-1]
				prevLogIndex = prevLogEntry.Index
				prevLogTerm = prevLogEntry.Term
			}

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     leaderId,
				Entries:      entries,
				LeaderCommit: currentCommitIndex,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
			}

			reply := AppendEntriesReply{
				Success: false,
			}

			// retry indefinitly till follower replies
			ok := false
			for !ok {
				ok = rf.sendAppendEntries(server, &args, &reply)
			}

			rf.mu.Lock()

			// check log incosistency
			for !reply.Success {
				fmt.Printf("[%d] current next index for server [%d] is %d\n", rf.me, server, rf.nextIndex[server])
				rf.nextIndex[server] -= 1
				nextEntryToSend := rf.log[rf.nextIndex[server]-1]
				prevIndex := rf.nextIndex[server] - 1

				if prevIndex > 0 {
					prevEntry := rf.log[prevIndex-1]
					args.PrevLogIndex = prevEntry.Index
					args.PrevLogTerm = prevEntry.Term
				} else {
					args.PrevLogIndex = 0
					args.PrevLogTerm = 0
				}

				newEntries := make([]LogEntry, len(args.Entries)+1)
				newEntries[0] = nextEntryToSend
				copy(newEntries[1:], args.Entries)
				args.Entries = newEntries
				args.LeaderCommit = rf.commitIndex

				rf.mu.Unlock()
				rf.sendAppendEntries(server, &args, &reply)
				rf.mu.Lock()
			}

			if reply.Success {
				rf.matchIndex[server] = logIndex
				rf.nextIndex[server] = logIndex + 1
				votes++
			}

			// check if majority replied
			if votes > len(rf.peers)/2 {
				rf.commitIndex = logIndex
				// reply to the client in channel
				reply := ApplyMsg{
					CommandValid: true,
					CommandIndex: logIndex,
					Command:      logEntryToBeReplicated.Command,
				}

				rf.mu.Unlock()

				go func(reply ApplyMsg) {
					rf.reply <- reply
				}(reply)

				rf.mu.Lock()
			}

			rf.mu.Unlock()

			if rf.state == Leader {
				rf.sendHeartbeats()
			}
		}(i, logIndex)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	// If I am not the leader, return false
	if rf.currentLeader != rf.me {
		return index, term, false
	}

	rf.mu.Lock()

	index = len(rf.log) + 1
	term = rf.currentTerm
	isLeader = rf.currentLeader == rf.me

	logEntry := LogEntry{Term: rf.currentTerm, Command: command, Index: index}
	rf.log = append(rf.log, logEntry)

	rf.mu.Unlock()

	// Send this log entry to followers
	go rf.logReplication(index)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.state != Leader {
			// check if hearbeat received from leader in last 1000 ms
			if time.Since(rf.lastPinged) > 500*time.Millisecond {
				// start election
				rf.mu.Unlock()
				rf.attemptElection()
				rf.mu.Lock()
			}
		} else {
			// send heartbeats to all followers to maintain leadership
			rf.mu.Unlock()
			rf.sendHeartbeats()
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.currentLeader = -1

	rf.state = Follower
	rf.lastPinged = time.Now()

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.reply = applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
