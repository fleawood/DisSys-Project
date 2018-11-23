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

func RandomTimeout() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
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

	newLeader chan bool
	Heartbeat chan bool
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
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	lastTerm := rf.GetLastTerm()
	lastIndex := rf.GetLastIndex()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastTerm || args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) handleRequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}
	if rf.state != CANDIDATE || reply.Term != rf.currentTerm {
		return
	}
	if reply.VoteGranted {
		//fmt.Printf("%d received a vote\n", rf.me)
		rf.numVotes ++
		if rf.numVotes > len(rf.peers)/2 {
			//fmt.Printf("%d is the new leader\n", rf.me)
			rf.newLeader <- true
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
		rf.handleRequestVote(args, reply)
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
	Term    int
	Success bool
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
	rf.Heartbeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) handleAppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}
	if rf.state != LEADER || args.Term != rf.currentTerm {
		return
	}
	if reply.Success {

	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Printf("%d send AppendEntries to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.handleAppendEntries(args, reply)
	}
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

func (rf *Raft) Loop() {
	for {
		switch rf.state {
		case FOLLOWER:
			select {
			case <-rf.Heartbeat:
				//fmt.Printf("%d received a heartbeat\n", rf.me)
			case <-time.After(RandomTimeout()):
				rf.state = CANDIDATE
				//fmt.Printf("%d now become a candidate\n", rf.me)
			}
		case LEADER:
			// TODO: broadcast
			rf.mu.Lock()
			for i := range rf.peers {
				if i != rf.me {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: -1,
						PrevLogTerm:  -1,
						Entries:      make([]LogEntry, 0),
						LeaderCommit: rf.commitIndex,
					}
					var reply AppendEntriesReply
					go rf.sendAppendEntries(i, args, &reply)
				}
			}
			rf.mu.Unlock()
			time.Sleep(time.Duration(50 * time.Millisecond))
		case CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm ++
			rf.votedFor = rf.me
			rf.numVotes = 1
			rf.mu.Unlock()
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
			select {
			case <-time.After(RandomTimeout()):
				//fmt.Printf("Election timeout\n")
			case <-rf.newLeader:
				rf.state = LEADER
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
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{0, 0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil

	rf.newLeader = make(chan bool, 1)
	rf.Heartbeat = make(chan bool, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Loop()
	return rf
}
