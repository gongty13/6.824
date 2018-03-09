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

import "sync"
import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"time"
)

// import "bytes"
// import "labgob"

const (
	FOLLOWER = iota
	LEADER
	CANDICATE
)

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	applyCh chan ApplyMsg
	timer   *time.Timer
	voteGot int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
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

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.lastApplied = -1
	rf.commitIndex = -1
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
	}
	rf.state = FOLLOWER
	rf.resetTimer()

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//println("voted", rf.me, args.CandidateId, reply.VoteGranted)
		return
	}
	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.resetTimer()
	}
	rf.persist()
	if rf.votedFor < 0 {
		reply.VoteGranted = true
		if len(rf.log) > 0 {
			lastLogTerm := rf.log[len(rf.log)-1].Term
			if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex) {
				reply.VoteGranted = false
			}
		}
		if reply.VoteGranted {
			rf.votedFor = args.CandidateId
			rf.persist()
		}

	}
	//println("voted", rf.me, args.CandidateId, reply.VoteGranted)
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type RequestAppendArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type RequestAppendReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendRequestAppend(server int, args *RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppend", args, reply)
	return ok
}
func (rf *Raft) RequestAppend(args *RequestAppendArgs, reply *RequestAppendReply) {
	//TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//println("req",rf.me,args.PrevLogIndex+len(args.Entries)+1)
	reply.Term = args.Term
	rf.currentTerm = args.Term
	rf.persist()
	if args.PrevLogIndex < 0 ||
		(args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
		reply.Success = true
		rf.votedFor = args.LeaderId
		rf.state = FOLLOWER
		for i := range args.Entries {
			if len(rf.log) <= args.PrevLogIndex+i+1 || rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex+i+1]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
		//rf.log = rf.log[:args.PrevLogIndex+1]
		//rf.log = append(rf.log, args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if rf.commitIndex >= len(rf.log) {
				rf.commitIndex = len(rf.log) - 1
			}
			rf.persist()
			//println("commit",rf.me, rf.commitIndex,len(rf.log))
			go rf.commitEntries()
		}
	}
	rf.resetTimer()
}

func (rf *Raft) commitEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		//println("commitp", rf.me, rf.lastApplied, len(rf.log), rf.currentTerm, rf.commitIndex)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		rf.persist()
		//println("commitpp", rf.me, rf.lastApplied, len(rf.log), rf.currentTerm)
		//println("commitppp", rf.me, rf.lastApplied, len(rf.log), rf.log[rf.lastApplied].Term, rf.currentTerm, rf.log[rf.lastApplied].Command.(int))
	}
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
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).
	if rf.state != LEADER {
		return -1, -1, false
	}
	//println("lll", rf.me, command.(int), len(rf.log))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index := len(rf.log)
	rf.persist()
	go rf.requestAppend()

	return index, rf.currentTerm, true
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.lastApplied = -1
	rf.commitIndex = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timer = time.NewTimer(time.Second)
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	go rf.handleTimer()
	return rf
}

func (rf *Raft) resetTimer() {
	switch rf.state {
	case LEADER:
		rf.timer.Reset(100 * time.Millisecond)
	default:
		rf.timer.Reset(time.Millisecond * time.Duration(200+rand.Int31n(200+1)))
	}
}
func (rf *Raft) handleTimer() {
	for {
		<-rf.timer.C
		rf.mu.Lock()
		switch rf.state {
		case LEADER:
			rf.resetTimer()
			rf.requestAppend()
		default:
			rf.state = CANDICATE
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteGot = 1
			rf.persist()
			rf.resetTimer()
			//println("voting", rf.me, rf.currentTerm)
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					go func(server int) {
						args := &RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateId:  rf.me,
							LastLogIndex: len(rf.log) - 1,
							LastLogTerm:  -1,
						}
						if len(rf.log) > 0 {
							args.LastLogTerm = rf.log[len(rf.log)-1].Term
						}
						reply := &RequestVoteReply{}
						if ok := rf.sendRequestVote(server, args, reply); ok {
							rf.handleVoteReply(reply)
						}
					}(server)
				}

			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.resetTimer()
	} else if reply.Term == rf.currentTerm && reply.VoteGranted {
		rf.voteGot++
		if rf.voteGot > len(rf.peers)/2 && rf.state == CANDICATE {
			rf.state = LEADER
			println("became leader", rf.me)
			for i := range rf.matchIndex {
				if i != rf.me {
					rf.matchIndex[i] = -1
					rf.nextIndex[i] = len(rf.log)
				}
			}
			rf.persist()
			rf.resetTimer()
			rf.requestAppend()
		}
	}
}

func (rf *Raft) requestAppendToOne(server int) {
	rf.mu.Lock()

	args := &RequestAppendArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  -1,
		LeaderCommit: rf.commitIndex,
		Entries:      nil,
	}

	if args.PrevLogIndex > -1 && args.PrevLogIndex < len(rf.log) {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	if args.PrevLogIndex+1 >= len(rf.log) {
		args.Entries = make([]LogEntry, 0)
	} else if rf.state == LEADER {
		//println("error?", len(rf.log), args.PrevLogIndex, rf.nextIndex[server])
		args.Entries = rf.log[args.PrevLogIndex+1:]
	}
	rf.mu.Unlock()
	if rf.state == LEADER {
		reply := &RequestAppendReply{
			Success: false,
		}
		if ok := rf.sendRequestAppend(server, args, reply); ok {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FOLLOWER
				rf.persist()
				rf.resetTimer()
			} else if reply.Success {
				matchIndex := args.PrevLogIndex + len(args.Entries)
				rf.matchIndex[server] = matchIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1

				if matchIndex > rf.commitIndex && rf.log[matchIndex].Term == rf.currentTerm {
					count := 1
					for i := range rf.matchIndex {
						if i != rf.me && rf.matchIndex[i] >= matchIndex {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = matchIndex
						go rf.commitEntries()
					}
				}
			} else if rf.nextIndex[server] > 0 {
				nextIndex := rf.nextIndex[server] - 1
				for nextIndex > 0 && rf.log[nextIndex].Term == rf.log[nextIndex-1].Term {
					nextIndex--
				}
				rf.nextIndex[server] = nextIndex
				go rf.requestAppendToOne(server)
			}
		} else {
			go rf.requestAppendToOne(server)
		}
	}
}

func (rf *Raft) requestAppend() {
	if rf.state == LEADER {
		for server := range rf.peers {
			if server != rf.me {
				go rf.requestAppendToOne(server)
			}
		}
	}
}
