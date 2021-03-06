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
	"fmt"
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
const DE_BUG = 0

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

	commitIndex          int
	lastApplied          int
	lastAppliedRealIndex int
	nextIndex            []int
	matchIndex           []int

	applyCh chan ApplyMsg
	timer   *time.Timer
	voteGot int

	CommandNumInSnap     int
	LastTermInSnap       int
	FakeCommandNumInSnap int
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
	e.Encode(rf.CommandNumInSnap)
	e.Encode(rf.LastTermInSnap)
	e.Encode(rf.FakeCommandNumInSnap)
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

	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.CommandNumInSnap) != nil ||
		d.Decode(&rf.LastTermInSnap) != nil ||
		d.Decode(&rf.FakeCommandNumInSnap) != nil {
	}
	rf.commitIndex = rf.CommandNumInSnap - 1
	rf.lastApplied = rf.commitIndex
	rf.lastAppliedRealIndex = rf.commitIndex - rf.FakeCommandNumInSnap

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
			if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && len(rf.log)-1+rf.CommandNumInSnap > args.LastLogIndex) {
				reply.VoteGranted = false
			}
		}
		if reply.VoteGranted {
			rf.votedFor = args.CandidateId
			lt := -1
			if len(rf.log) > 0 {
				lt = rf.log[len(rf.log)-1].Term
			}
			if DE_BUG > 0 {
				println(rf.me, "vote for", args.CandidateId, "index", len(rf.log)-1, args.LastLogIndex, "term", lt, rf.LastTermInSnap, args.LastLogTerm, args.Term)
			}
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
	reply.Success = false
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.resetTimer()
		rf.persist()
	} else if rf.currentTerm == args.Term && rf.state == LEADER {
		return
	}
	//println("append", args.PrevLogIndex, rf.CommandNumInSnap, len(rf.log), "me:", rf.me, "size:", rf.StateSize())
	if (len(args.Entries) == 0 || args.Entries[len(args.Entries)-1].Term == rf.currentTerm) && ((args.PrevLogIndex == rf.CommandNumInSnap-1 && args.PrevLogTerm == rf.LastTermInSnap) ||
		(args.PrevLogIndex >= rf.CommandNumInSnap && args.PrevLogIndex < len(rf.log)+rf.CommandNumInSnap && rf.log[args.PrevLogIndex-rf.CommandNumInSnap].Term == args.PrevLogTerm)) {
		rf.votedFor = args.LeaderId
		rf.state = FOLLOWER
		for i := range args.Entries {
			if len(rf.log)+rf.CommandNumInSnap <= args.PrevLogIndex+i+1 || rf.log[args.PrevLogIndex-rf.CommandNumInSnap+1+i].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex-rf.CommandNumInSnap+i+1]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		}
		reply.Success = true
		//rf.log = rf.log[:args.PrevLogIndex+1]
		//rf.log = append(rf.log, args.Entries...)
		aLeaderCommit := args.LeaderCommit
		if aLeaderCommit > args.PrevLogIndex+len(args.Entries) {
			aLeaderCommit = args.PrevLogIndex + len(args.Entries)
		}
		if aLeaderCommit > rf.commitIndex {
			rf.commitIndex = aLeaderCommit

			if rf.commitIndex >= len(rf.log)+rf.CommandNumInSnap {
				rf.commitIndex = len(rf.log) + rf.CommandNumInSnap - 1
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
		//println("commit me", rf.me, "last:", rf.lastApplied, "len:", len(rf.log), "com:", rf.commitIndex, "snap", rf.CommandNumInSnap, rf.state == LEADER, rf.currentTerm)
		if v, ok := rf.log[rf.lastApplied-rf.CommandNumInSnap].Command.(int); !ok || v != -1 {
			rf.lastAppliedRealIndex++
			//println("commit me", rf.me, "last:", rf.lastApplied, "index:", rf.lastAppliedRealIndex, "len:", len(rf.log), "com:", rf.commitIndex, "snap", rf.CommandNumInSnap, rf.state == LEADER, rf.currentTerm, "val:", v)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.CommandNumInSnap].Command,
				CommandIndex: rf.lastAppliedRealIndex + 1,
			}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}
	//println("lll", rf.me, command.(int), len(rf.log))
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index := rf.Info() + rf.CommandNumInSnap - rf.FakeCommandNumInSnap
	//println("start", rf.me, command.(int), len(rf.log), index, rf.currentTerm, rf.state == LEADER)
	rf.persist()
	go rf.requestAppend()
	//for rf.commitIndex+1 < index {
	//	if rf.state != LEADER {
	//		return -1, -1, false
	//	}
	//}
	return index, rf.currentTerm, true
}

func (rf *Raft) StartV2(command interface{}) (int, int, bool) {
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).
	if rf.state != LEADER {
		return -1, -1, false
	}
	//println("lll", rf.me, command.(int), len(rf.log))
	rf.mu.Lock()
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	println("v2", len(rf.log))
	index := len(rf.log) - 1 + rf.CommandNumInSnap
	defer rf.mu.Unlock()
	rf.persist()
	go rf.requestAppend()
	//for rf.commitIndex+1 < index {
	//	if rf.state != LEADER {
	//		return -1, -1, false
	//	}
	//}
	return index, rf.currentTerm, true
}

func (rf *Raft) GetCommand(index int) (interface{}, bool) {
	if index < rf.CommandNumInSnap {
		return nil, false
	}
	if index < rf.CommandNumInSnap+len(rf.log) {
		return rf.log[index-rf.CommandNumInSnap].Command, true
	}
	return nil, false
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
	rf.lastAppliedRealIndex = -1
	rf.commitIndex = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timer = time.NewTimer(time.Second)

	rf.LastTermInSnap = -1
	rf.CommandNumInSnap = 0
	rf.FakeCommandNumInSnap = 0
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
							LastLogIndex: len(rf.log) + rf.CommandNumInSnap - 1,
							LastLogTerm:  -1,
						}
						if len(rf.log) > 0 {
							args.LastLogTerm = rf.log[len(rf.log)-1].Term
						} else if rf.CommandNumInSnap > 0 {
							args.LastLogIndex = rf.LastTermInSnap
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
			if DE_BUG > 0 {
				println("became leader", rf.me, rf.Info()+rf.CommandNumInSnap-rf.FakeCommandNumInSnap, len(rf.log)+rf.CommandNumInSnap, rf.currentTerm)
				if len(rf.log) > 0 {
					fmt.Println(rf.log[len(rf.log)-1])
				}
			}

			for i := range rf.matchIndex {
				if i != rf.me {
					rf.matchIndex[i] = -1
					rf.nextIndex[i] = len(rf.log) + rf.CommandNumInSnap
				}
			}
			rf.log = append(rf.log, LogEntry{-1, rf.currentTerm})
			rf.persist()
			rf.resetTimer()
			rf.requestAppend()
		}
	}
}

func (rf *Raft) requestInstallSnapshotToOne(server int) {
	rf.mu.Lock()
	//println("sending snap", rf.CommandNumInSnap, len(rf.log), "to:", server, "from:", rf.me)
	var args InstallSnapshotArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.CommandNumInSnap
	args.LastIncludedTerm = rf.LastTermInSnap
	args.FakeIndex = rf.FakeCommandNumInSnap
	args.Data = rf.persister.snapshot
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", &args, &reply); ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.persist()
			rf.resetTimer()
		} else {
			rf.matchIndex[server] = rf.CommandNumInSnap - 1
			rf.nextIndex[server] = rf.CommandNumInSnap
		}
	} else {
		go rf.requestInstallSnapshotToOne(server)
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
	if rf.CommandNumInSnap > 0 && args.PrevLogIndex+1 < rf.CommandNumInSnap {
		go rf.requestInstallSnapshotToOne(server)
		rf.mu.Unlock()
		return
	}

	if args.PrevLogIndex > -1 && args.PrevLogIndex < len(rf.log)+rf.CommandNumInSnap {
		//println("pre", args.PrevLogIndex, rf.CommandNumInSnap, len(rf.log), "to:", server, "from:", rf.me)
		if args.PrevLogIndex >= rf.CommandNumInSnap {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.CommandNumInSnap].Term
		} else {
			args.PrevLogTerm = rf.LastTermInSnap
		}
	}
	if args.PrevLogIndex+1 >= len(rf.log)+rf.CommandNumInSnap {
		args.Entries = make([]LogEntry, 0)
	} else if rf.state == LEADER {
		//println("error?", len(rf.log), args.PrevLogIndex, rf.nextIndex[server])
		args.Entries = rf.log[args.PrevLogIndex+1-rf.CommandNumInSnap:]
	}
	//defer
	rf.mu.Unlock()
	if rf.state == LEADER {
		reply := &RequestAppendReply{
			Success: false,
		}
		if ok := rf.sendRequestAppend(server, args, reply); ok {
			rf.mu.Lock()
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

				if matchIndex > rf.commitIndex && rf.state == LEADER && rf.log[matchIndex-rf.CommandNumInSnap].Term == rf.currentTerm {
					count := 1
					for i := range rf.matchIndex {
						if i != rf.me && rf.matchIndex[i] >= matchIndex {
							count++
						}
					}
					if count > len(rf.peers)/2 && rf.state == LEADER {
						rf.commitIndex = matchIndex
						//fmt.Println("match:", rf.matchIndex, matchIndex, count, "next:", rf.nextIndex, rf.currentTerm)
						go rf.commitEntries()
					}
				}
			} else if rf.nextIndex[server] > 0 && rf.state == LEADER {
				nextIndex := rf.nextIndex[server] - 1 - rf.CommandNumInSnap

				for nextIndex > 0 && rf.log[nextIndex].Term == rf.log[nextIndex-1].Term {
					nextIndex--
				}
				rf.nextIndex[server] = nextIndex + rf.CommandNumInSnap
				go rf.requestAppendToOne(server)
			}
			rf.mu.Unlock()
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

func (rf *Raft) countFake(n int) int {
	ret := 0
	for i := 0; i < n; i++ {
		if v, ok := rf.log[i].Command.(int); ok && v == -1 {
			ret++
		}
	}
	return ret
}
func (rf *Raft) Info() int {
	ret := 0
	for i := range rf.log {
		if v, ok := rf.log[i].Command.(int); !ok || v != -1 {
			ret++
		}
	}
	return ret
}
func (rf *Raft) Detail() map[string]int {
	d := make(map[string]int)
	d["l"] = len(rf.log)
	d["t"] = rf.currentTerm
	d["c"] = rf.commitIndex
	d["s"] = rf.CommandNumInSnap
	return d
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	FakeIndex         int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//println("installing snap", rf.me, args.Term, rf.currentTerm, rf.commitIndex, args.LastIncludedIndex, rf.CommandNumInSnap)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.resetTimer()
		rf.persist()
	}
	reply.Term = args.Term
	if args.LastIncludedIndex < rf.CommandNumInSnap {
		return
	}
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex - 1
		//rf.commitEntries()
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex - 1
		rf.lastAppliedRealIndex = rf.lastApplied - args.FakeIndex
	}

	if len(rf.log)+rf.CommandNumInSnap <= args.LastIncludedIndex {
		rf.log = make([]LogEntry, 0)
	} else {
		rf.log = rf.log[args.LastIncludedIndex-rf.CommandNumInSnap:]
	}

	rf.CommandNumInSnap = args.LastIncludedIndex
	rf.LastTermInSnap = args.LastIncludedTerm
	rf.FakeCommandNumInSnap = args.FakeIndex
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	rf.resetTimer()
}

func (rf *Raft) PrepareSnapShot(data map[string]string, opCommit map[int64]int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(data)
	e.Encode(opCommit)
	//println("snaping", len(rf.log), rf.lastApplied, rf.commitIndex, rf.CommandNumInSnap)
	rf.LastTermInSnap = rf.log[rf.commitIndex-rf.CommandNumInSnap].Term
	rf.FakeCommandNumInSnap += rf.countFake(rf.commitIndex - rf.CommandNumInSnap + 1)
	rf.log = rf.log[rf.commitIndex-rf.CommandNumInSnap+1:]
	rf.CommandNumInSnap = rf.commitIndex + 1
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, w.Bytes())
	for i := range rf.peers {
		if i != rf.me {
			go rf.requestInstallSnapshotToOne(i)
		}
	}
}

func (rf *Raft) sendSnapShotToOne(server int) {
	var args InstallSnapshotArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.CommandNumInSnap
	args.LastIncludedTerm = rf.LastTermInSnap
	args.FakeIndex = rf.FakeCommandNumInSnap
	args.Data = rf.persister.snapshot
	var reply InstallSnapshotReply
	for {
		if ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", &args, &reply); true {
			println("ok", ok)
			if ok {
				break
			}
		}
	}
}

func (rf *Raft) StateSize() int {
	return rf.persister.RaftStateSize()
}
