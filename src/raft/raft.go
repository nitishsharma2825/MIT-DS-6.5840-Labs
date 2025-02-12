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

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

	applyCh chan ApplyMsg

	voteCount int

	// last snapshot metadata
	lastIncludedIndex int
	lastIncludedTerm  int
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

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getCurrentLogIndex(index int) int {
	return index - rf.lastIncludedIndex
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.Save(raftState, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var log []LogEntry
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Printf("Error in Decoding\n")
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Till where can the log be trimmed on this peer? lastApplied?
	// index should be <= commitIndex of this peer
	// if service has issued this snapshot, it must be >= lastApplied
	if index <= rf.lastIncludedIndex || rf.lastApplied != rf.commitIndex {
		return
	}

	newLog := make([]LogEntry, len(rf.log)-rf.getCurrentLogIndex(index))
	copy(newLog, rf.log[index-rf.lastIncludedIndex:])

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[0].Term

	rf.persistSnapshot(snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
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
	Term       int
	Success    bool
	MatchIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

// helper function to step down to follower and update current term
func (rf *Raft) stepDownToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteCount = 0
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// If candidate's term < my term, do not grant vote
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// If candidate's term > my term, step down to follower and continue to check for granting vote
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	myLastLogIndex := rf.getLastIndex()
	myLastLogTerm := rf.getLastTerm()

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		grantVote := false

		// does candidate have a higher term?
		if args.LastLogTerm > myLastLogTerm {
			grantVote = true
		}

		// If term is same, does candidate has bigger log
		if args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex {
			grantVote = true
		}

		if grantVote {
			reply.VoteGranted = true
			reply.Term = args.Term

			rf.votedFor = args.CandidateId
			rf.state = Follower
			rf.lastPinged = time.Now()
			rf.currentTerm = args.Term
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.lastPinged = time.Now()

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	myLastLogIndex := rf.getLastIndex()
	argsPrevIndex := rf.getCurrentLogIndex(args.PrevLogIndex)

	// my log is smaller than leader's log
	// or leader's log index is present but term does not match
	// ask leader to send previous log to find match history
	if myLastLogIndex < args.PrevLogIndex || rf.log[argsPrevIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.MatchIndex = rf.commitIndex + 1
		return
	}

	// update my log
	hasLogDiverged := false
	for _, logEntry := range args.Entries {

		// check if my logs needs to be overwritten
		// if yes, find the last matching index and update everything after that
		if myLastLogIndex >= logEntry.Index {
			idx := rf.getCurrentLogIndex(logEntry.Index)
			if hasLogDiverged {
				rf.log[idx] = logEntry
			} else {
				if rf.log[idx].Term == logEntry.Term {
					continue
				} else {
					hasLogDiverged = true
					rf.log[idx] = logEntry
				}
			}
		} else {
			rf.log = append(rf.log, logEntry)
		}
	}

	if len(args.Entries) > 0 {
		lastEntryIndex := args.Entries[len(args.Entries)-1].Index
		// delete extra entries in the follower log
		// to make it consistent with leader log
		if myLastLogIndex > lastEntryIndex {
			rf.log = rf.log[:rf.getCurrentLogIndex(lastEntryIndex)+1]
		}
	}

	rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())

	reply.Success = true
	reply.Term = rf.currentTerm

	go rf.applyCommits()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}

	rf.lastPinged = time.Now()
	reply.Term = args.Term

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	rf.currentLeader = args.LeaderId

	// data is already present
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.persist()
		return
	}

	lastIndex := rf.getLastIndex()

	// check how much log can be discarded
	if args.LastIncludedIndex > lastIndex || rf.log[rf.getCurrentLogIndex(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
		// entire log can be discarded
		rf.log = []LogEntry{
			{
				Index:   args.LastIncludedIndex,
				Command: nil,
				Term:    args.LastIncludedTerm,
			},
		}
	} else {
		// discard the log till args.lastIncludedIndex
		rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persistSnapshot(args.Snapshot)

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}

	rf.applyCh <- msg
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

func (rf *Raft) sendRequestVoteHelper(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		// Request timed out, no response
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	servers := len(rf.peers)

	// check if the term has changed since the request was sent, or if the server is no longer a candidate
	if args.Term != rf.currentTerm || rf.state != Candidate || reply.Term < rf.currentTerm {
		return
	}

	rf.lastPinged = time.Now()

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
	}

	if reply.VoteGranted {
		rf.voteCount++
	}

	if rf.voteCount > (servers / 2) {
		rf.state = Leader
		rf.lastPinged = time.Now()
		rf.currentLeader = rf.me

		// Reinitialized after election
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.getLastIndex() + 1
			rf.matchIndex[i] = 0
		}

		rf.sendUpdates()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesHelper(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
		rf.lastPinged = time.Now()
		return
	}

	if reply.Success {
		// not an heartbeat but an actual append entry request
		// update nextIndex and matchIndex
		if args.Entries != nil {
			lastEntrySent := args.Entries[len(args.Entries)-1]
			rf.nextIndex[server] = lastEntrySent.Index + 1
			rf.matchIndex[server] = lastEntrySent.Index
		}
	} else {
		// decrement the nextIndex and try again
		rf.nextIndex[server] = reply.MatchIndex
	}

	// Check if max votes received and can commit?

	// find highest index known to be replicated on all servers
	// this will be a candidate to commit
	cnt := map[int]int{}
	maxIndexPresentOnAllServers := 0
	maxIndexCanBeCommited := 0

	for i, v := range rf.matchIndex {
		if i == rf.me {
			continue
		}

		cnt[v]++
		if cnt[v] > maxIndexPresentOnAllServers {
			maxIndexPresentOnAllServers = cnt[v]
			maxIndexCanBeCommited = v
		} else if cnt[v] == maxIndexPresentOnAllServers {
			maxIndexCanBeCommited = max(v, maxIndexCanBeCommited)
		}
	}

	// check if this candidate is present on majority servers
	if maxIndexPresentOnAllServers >= (len(rf.peers)-1)/2 {
		if maxIndexCanBeCommited > rf.commitIndex && rf.log[rf.getCurrentLogIndex(maxIndexCanBeCommited)].Term == rf.currentTerm {
			rf.commitIndex = maxIndexCanBeCommited
		}
	}

	// send update to application
	go rf.applyCommits()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
		rf.lastPinged = time.Now()
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

	go rf.applyCommits()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader {
		return index, term, false
	}

	index = rf.getLastIndex() + 1
	term = rf.currentTerm
	isLeader = true

	logEntry := LogEntry{Term: rf.currentTerm, Command: command, Index: index}
	rf.log = append(rf.log, logEntry)

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

func (rf *Raft) sendUpdates() {
	// send updates to peers based on nextIndex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] <= rf.lastIncludedIndex {

			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Snapshot:          rf.persister.ReadSnapshot(),
			}

			reply := InstallSnapshotReply{}

			go rf.sendInstallSnapshot(i, &args, &reply)

			continue
		}

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogIndexCurrentLog := rf.getCurrentLogIndex(prevLogIndex)
		prevLogTerm := rf.log[prevLogIndexCurrentLog].Term
		newEntries := append([]LogEntry(nil), rf.log[prevLogIndexCurrentLog+1:]...)

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      newEntries,
			LeaderCommit: rf.commitIndex,
		}

		reply := AppendEntriesReply{}

		go rf.sendAppendEntriesHelper(i, &args, &reply)
	}
}

func (rf *Raft) applyCommits() {
	rf.mu.Lock()

	if rf.lastApplied >= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	var entriesToApply []ApplyMsg

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		replyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.getCurrentLogIndex(i)].Command,
			CommandIndex: i,
		}

		entriesToApply = append(entriesToApply, replyMsg)
	}

	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	for _, applyMsg := range entriesToApply {
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) attemptElection() {

	defer rf.persist()

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.lastPinged = time.Now()
	rf.voteCount = 1

	term := rf.currentTerm
	servers := len(rf.peers)

	for i := 0; i < servers; i++ {
		if i == rf.me {
			continue
		}

		// can't simply take len(rf.log)-1 after snapshots
		lastLogIdx := rf.getLastIndex()
		lastLogTerm := rf.getLastTerm()

		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIdx,
			LastLogTerm:  lastLogTerm,
		}

		reply := RequestVoteReply{}

		go rf.sendRequestVoteHelper(i, &args, &reply)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		if rf.state != Leader {
			// check if hearbeat received from leader in last 500 ms
			timeDiff := time.Since(rf.lastPinged).Milliseconds() + int64(rand.Intn(10)*20)
			if timeDiff >= 1000 {
				// start election
				rf.attemptElection()
			}
		} else {
			// send updates to all followers to maintain leadership
			rf.sendUpdates()
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 150 + (rand.Int63()%10)*10
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

	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.log = []LogEntry{{Term: 0, Command: nil, Index: 0}}

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
