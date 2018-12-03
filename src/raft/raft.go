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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

const BROADCASTTIME = 40 * time.Millisecond

func RandomTimeout() time.Duration {
	return time.Duration(rand.Intn(300)+400) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	state       int
	commitIndex int
	lastApplied int
	numVotes    int

	nextIndex  []int
	matchIndex []int

	resetTimeout chan bool
	//heartBeatCh  chan bool
	//grantVoteCh  chan bool
	toFollowerCh chan bool
	newLeader    bool
	newLeaderCh  chan bool
	newMatch     bool
	newMatchCh   chan bool
	//newCommand   bool
	//newCommandCh chan bool
	newCommit   bool
	newCommitCh chan bool
	applyMsgCh  chan ApplyMsg
}

func (rf *Raft) GetLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) GetLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		//fmt.Printf("%d's term %d -> %d\n", rf.me, rf.currentTerm, args.Term)
		//fmt.Println(rf.me, rf.state)
		rf.currentTerm = args.Term
		if rf.state == LEADER {
			rf.toFollowerCh <- true
		}
		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
		} else {
			rf.resetTimeout <- true
		}
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	lastTerm := rf.GetLastTerm()
	lastIndex := rf.GetLastIndex()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastTerm || args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
		if rf.state == LEADER {
			rf.toFollowerCh <- true
		}
		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
		} else {
			rf.resetTimeout <- true
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) handleRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.VoteGranted {
		//fmt.Println(server, "vote for", rf.me)
	} else {
		//fmt.Println(server, "do not vote for", rf.me)
	}
	if reply.Term > rf.currentTerm {
		//fmt.Printf("%d's term %d -> %d\n", rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		if rf.state == LEADER {
			rf.toFollowerCh <- true
		}
		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
		} else {
			rf.resetTimeout <- true
		}
		rf.votedFor = -1
		rf.persist()
		return
	}
	if rf.state != CANDIDATE || reply.Term != rf.currentTerm || rf.newLeader {
		return
	}
	if reply.VoteGranted {
		//fmt.Printf("%d received a vote\n", rf.me)
		rf.numVotes ++
		if rf.numVotes > len(rf.peers)/2 {
			//fmt.Printf("%d is the new leader\n", rf.me)
			rf.newLeader = true
			rf.newLeaderCh <- true
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Printf("%d send RequestVote to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		//fmt.Printf("%d succeed to send RequestVote to %d\n", rf.me, server)
		rf.handleRequestVote(server, args, reply)
	} else {
		//fmt.Printf("%d failed to send RequestVote to %d\n", rf.me, server)
	}
	return ok
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
	Term          int
	Success       bool
	conflictIndex int
	conflictTerm  int
}

func (rf *Raft) makeAppendEntriesArgs(i int) AppendEntriesArgs {
	var entries []LogEntry
	if rf.GetLastIndex() >= rf.nextIndex[i] {
		entries = rf.log[rf.nextIndex[i]:]
	} else {
		entries = make([]LogEntry, 0)
	}
	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//if args.Term > rf.currentTerm {
	//	rf.currentTerm = args.Term
	//	if rf.state == LEADER {
	//		rf.toFollowerCh <- true
	//	}
	//	if rf.state != FOLLOWER {
	//		rf.state = FOLLOWER
	//	} else {
	//		//rf.resetTimeout <- true
	//	}
	//}
	if args.Term > rf.currentTerm || args.Term == rf.currentTerm && rf.state == CANDIDATE {
		//fmt.Printf("%d's term %d -> %d\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		if rf.state == LEADER {
			rf.toFollowerCh <- true
		}
		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
		} else {
			//rf.resetTimeout <- true
		}
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.conflictIndex = len(rf.log)
		reply.conflictTerm = -1
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.conflictTerm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex - 1; i >= 0; i -- {
			if rf.log[i].Term != reply.conflictTerm {
				reply.conflictIndex = i + 1
			}
		}
		return
	}
	reply.Success = true
	rf.resetTimeout <- true
	for i := range args.Entries {
		if args.PrevLogIndex+1+i >= len(rf.log) || rf.log[args.PrevLogIndex+1+i] != args.Entries[i] {
			rf.log = append(rf.log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
			//fmt.Println(rf.me, rf.log)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastIndex())
		if !rf.newCommit {
			rf.newCommit = true
			rf.newCommitCh <- true
		}
		//rf.newCommitCh <- true
		//fmt.Printf("update %d commitIndex to %d\n", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) handleAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		//fmt.Printf("%d's term %d -> %d from %d\n", rf.me, rf.currentTerm, reply.Term, server)
		rf.currentTerm = reply.Term
		if rf.state == LEADER {
			rf.toFollowerCh <- true
		}
		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
		}
		rf.votedFor = -1
		rf.persist()
		return false
	}
	if rf.state != LEADER || args.Term != rf.currentTerm {
		return false
	}
	if reply.Success {
		n := len(args.Entries)
		if n > 0 {
			//fmt.Println(rf.me, server, args.Entries)
			rf.nextIndex[server] = max(rf.nextIndex[server], args.Entries[n-1].Index+1)
			//rf.matchIndex[server] = max(rf.matchIndex[server], args.Entries[n-1].Index)
			if !rf.newMatch {
				rf.newMatch = true
				rf.newMatchCh <- true
			}
			//fmt.Printf("%d %d\n", rf.me, server)
		}
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		return false
	} else {
		if reply.conflictTerm == -1 {
			rf.nextIndex[server] = reply.conflictIndex
		} else {
			for i := args.PrevLogIndex; i >= 0; i -- {
				if rf.log[i].Term == reply.conflictTerm {
					rf.nextIndex[server] = i + 1
					break
				}
			}
		}
		return true
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Printf("%d send AppendEntries to %d\n", rf.me, server)
	var ok bool
	for retry := true; retry; {
		retry = false
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			//fmt.Println(rf.me, "succeed to send to", server)
			if rf.handleAppendEntries(server, args, reply) {
				retry = true
				//rf.nextIndex[server] --
				args = rf.makeAppendEntriesArgs(server)
			}
		} else {
			//fmt.Println(rf.me, "failed to send to", server)
		}
	}
	//ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//if ok {
	//	if rf.handleAppendEntries(server, args, reply) {
	//		rf.nextIndex[server] --
	//		args.PrevLogIndex = rf.nextIndex[server]
	//		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	//
	//	}
	//}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.GetLastIndex() + 1
		//fmt.Printf("index %d\n", index)
		rf.log = append(rf.log, LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		})
		//fmt.Println(rf.me, rf.log)
		rf.persist()
		//if !rf.newCommand {
		//	rf.newCommand = true
		//	rf.newCommandCh <- true
		//}
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) broadCast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		//fmt.Printf("%d send heartbeat to %d\n", rf.me, i)
		if i != rf.me {
			args := rf.makeAppendEntriesArgs(i)
			var reply AppendEntriesReply
			go rf.sendAppendEntries(i, args, &reply)
		}
	}
	//fmt.Println(rf.me, "next", rf.nextIndex)
	//fmt.Println(rf.me, "match", rf.matchIndex)
}

func (rf *Raft) periodicBroadCast() {
	for {
		if rf.state != LEADER {
			return
		}
		rf.broadCast()
		time.Sleep(BROADCASTTIME)
	}
}

func (rf *Raft) ApplyCheck() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.newCommit = false
	for ; rf.lastApplied < rf.commitIndex; {
		//fmt.Printf("applycheck %d\n", rf.me)
		rf.lastApplied ++
		rf.applyMsgCh <- ApplyMsg{
			Index:   rf.lastApplied,
			Command: rf.log[rf.lastApplied].Command,
		}
	}
}

func (rf *Raft) ApplyCheckLoop() {
	for {
		<-rf.newCommitCh
		rf.ApplyCheck()
	}
}

func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}
	rf.newMatch = false
	for N := rf.commitIndex + 1; N < len(rf.log); N ++ {
		numMatched := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
				numMatched ++
			}
		}
		if numMatched > len(rf.peers)/2 {
			rf.commitIndex = N
			//fmt.Printf("%d commitIndex %d\n", rf.me, rf.commitIndex)
			if !rf.newCommit {
				rf.newCommit = true
				rf.newCommitCh <- true
			}
		}
	}
}

//func (rf *Raft) broadCast() {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	rf.newCommand = false
//	//fmt.Printf("%d broadcast\n", rf.me)
//	fmt.Println(rf.me, rf.log)
//	for i := range rf.peers {
//		if i != rf.me && rf.GetLastIndex() >= rf.nextIndex[i] {
//			//fmt.Printf("%d broadcast to %d\n", rf.me, i)
//			args := rf.makeAppendEntriesArgs(i, false)
//			var reply AppendEntriesReply
//			go rf.sendAppendEntries(i, args, &reply)
//		}
//	}
//	//fmt.Println("broadcast end")
//}

func (rf *Raft) holdElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state != CANDIDATE {
		return
	}
	//fmt.Println(rf.me, "hold election")
	rf.currentTerm ++
	rf.votedFor = rf.me
	rf.numVotes = 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLastIndex(),
		LastLogTerm:  rf.GetLastTerm(),
	}
	for i := range rf.peers {
		if i != rf.me {
			var reply RequestVoteReply
			go rf.sendRequestVote(i, args, &reply)
		}
	}
	//fmt.Println(rf.me, "send all request vote")
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.newLeader = false
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.GetLastIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.nextIndex[rf.me] = -1
	rf.matchIndex[rf.me] = -1
	//fmt.Println(rf.log)
	go rf.periodicBroadCast()
}

func (rf *Raft) Loop() {
	for {
		switch rf.state {
		case FOLLOWER:
			select {
			case <-rf.resetTimeout:
				//fmt.Printf("%d reset timeout\n", rf.me)
			case <-time.After(RandomTimeout()):
				rf.state = CANDIDATE
				//fmt.Printf("%d now become a candidate in term %d\n", rf.me, rf.currentTerm)
			//case <-rf.toFollowerCh:
			//	fmt.Println("Should not occur!\n")
			}
		case LEADER:
			select {
			case <-rf.newMatchCh:
				rf.updateCommit()
			case <-rf.toFollowerCh:
				//fmt.Printf("leader %d become a follower\n", rf.me)
			}
		case CANDIDATE:
			rf.holdElection()
			select {
			case <-time.After(RandomTimeout()):
				//fmt.Printf("%d Election timeout\n", rf.me)
			case <-rf.newLeaderCh:
				//fmt.Printf("%d become the new leader in term %d\n", rf.me, rf.currentTerm)
				rf.becomeLeader()
			//case <-rf.toFollowerCh:
				//fmt.Printf("candidate %d become a follower", rf.me)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{0, 0, nil})

	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil

	rf.resetTimeout = make(chan bool, 1)
	//rf.heartBeatCh = make(chan bool, 1)
	//rf.grantVoteCh = make(chan bool, 1)
	rf.toFollowerCh = make(chan bool, 1)
	rf.newMatch = false
	rf.newMatchCh = make(chan bool, 1)
	rf.newLeader = false
	rf.newLeaderCh = make(chan bool, 1)
	//rf.newCommand = false
	//rf.newCommandCh = make(chan bool, 1)
	rf.newCommit = false
	rf.newCommitCh = make(chan bool, 1)
	rf.applyMsgCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Loop()
	go rf.ApplyCheckLoop()
	return rf
}
