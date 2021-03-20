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
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

type LogEntry struct {
	Term    int // log term
	Index   int // log index
	Content interface{}
}

// raft state enum
type electionState int

const (
	follower electionState = iota
	candidate
	leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                    sync.Mutex          // Lock to protect shared access to this peer's state
	peers                 []*labrpc.ClientEnd // RPC end points of all peers
	persister             *Persister          // Object to hold this peer's persisted state
	me                    int                 // this peer's index into peers[]
	dead                  int32               // set by Kill()
	currentTerm           int                 // state, current term
	votedFor              int                 // state, voted id in current term, -1 for nil.
	logEntries            []*LogEntry         // log entry list
	commitIndex           int                 // index of highest log entry known to be committed
	lastApplied           int                 // index of highest log entry applied to state machine
	heartBeatInterval     int                 // heart beat interval, ms
	leaderElectionTimeout int                 // leader election delay timeout, ms
	isLeader              bool                // leader mark
	nextIndex             []int               // leader state, next index for each server
	matchIndex            []int               // leader state, index of highest log entry
	state                 electionState       // leader election state, follower, candidate, leader
	lastHeartbeatTime     time.Time           // time for follower get last heartbeat from leader
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) becomeFollower() {
	log.Printf("%v become follower", rf.me)
	rf.state = follower
	rf.votedFor = -1
	rf.isLeader = false
}

func (rf *Raft) becomeCandidate() {
	log.Printf("%v become candidate", rf.me)
	rf.state = candidate
	rf.votedFor = rf.me
	rf.lastHeartbeatTime = time.Now()
	rf.currentTerm++
	rf.isLeader = false
}

func (rf *Raft) becomeLeader() {
	log.Printf("%v become leader, lastheartbeat %v", rf.me, rf.lastHeartbeatTime)
	rf.state = leader
	rf.votedFor = -1
	rf.isLeader = true
	rf.lastHeartbeatTime = time.Now()
	go rf.heartBeats()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.isLeader
	// Your code here (2A).
	return term, isleader
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// var candidateLastIndex = args.lastLogIndex
	// var candidateLastLogTerm = args.lastLogTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var candidateTerm = args.Term
	var candidateId = args.CandidateID
	if rf.isLeader {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if candidateTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		if rf.votedFor == -1 || rf.votedFor == candidateId {
			rf.votedFor = candidateId
			reply.VoteGranted = true
			reply.Term = candidateTerm
		} else {
			reply.VoteGranted = false
			reply.Term = candidateTerm
		}
		return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, replyChans chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	replyChans <- reply
	return ok
}

func (rf *Raft) startRequestVote() {
	rf.mu.Lock()
	// change self votedFor
	rf.votedFor = rf.me
	votedCount := 0
	failedCount := 0
	replyChans := make(chan *RequestVoteReply)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		lastLogTerm := -1
		lastLogIndex := -1
		if rf.logEntries != nil && len(rf.logEntries) >= 0 {
			lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
			lastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
		}
		request := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogTerm:  lastLogTerm,
			LastLogIndex: lastLogIndex,
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &request, &reply, replyChans)
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	for votedCount < len(rf.peers)/2 && failedCount < len(rf.peers)/2 {
		rf.mu.Lock()
		nowTerm := rf.currentTerm
		rf.mu.Unlock()
		if currentTerm != nowTerm {
			break
		}
		select {
		case reply := <-replyChans:
			log.Printf("%v get reply %v", rf.me, reply)
			if reply.VoteGranted {
				votedCount++
			} else {
				failedCount++
			}
		case <-time.After(1000 * time.Millisecond):
			log.Printf("%v get no reply, %v", rf.me, time.Now().UnixNano()/int64(time.Millisecond))
			failedCount++
			continue
		}
	}
	rf.mu.Lock()
	if votedCount >= len(rf.peers)/2 {
		rf.becomeLeader()
	} else {
		rf.becomeFollower()
	}
	rf.mu.Unlock()
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

// handle appendEntries request
func (rf *Raft) RequestAppendEntriesHandler(request *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if request.Entries == nil || len(request.Entries) == 0 {
		// heartbeat request
		if rf.isLeader {
			if rf.currentTerm <= request.Term {
				reply.Term = request.Term
				reply.Success = true
				rf.lastHeartbeatTime = time.Now()
				rf.currentTerm = request.Term
				rf.becomeFollower()
				return
			} else {
				reply.Term = rf.currentTerm
				reply.Success = false
				return
			}
		}
		if rf.currentTerm > request.Term {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.lastHeartbeatTime = time.Now()
		rf.currentTerm = request.Term
		rf.becomeFollower()
		return
	} else {
		// not heartbeat request
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
}

func (rf *Raft) AppendEntries(server int, request *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply, replyChans chan *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntriesHandler", request, reply)
	if !ok {
		reply.Success = false
	}
	replyChans <- reply
	return ok
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	heartBeatRequest := RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.logEntries) - 1,
		PrevLogTerm:  rf.currentTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	replyChans := make(chan *RequestAppendEntriesReply)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		heartBeatReply := RequestAppendEntriesReply{}
		go rf.AppendEntries(i, &heartBeatRequest, &heartBeatReply, replyChans)
	}
	// todo
	// what we could do if cloud not get response from followers.
	// time.Sleep(time.Duration(rf.heartBeatInterval) * time.Millisecond)
	successCount := 0
	failedCount := 0
	for successCount < len(rf.peers)/2 && failedCount < len(rf.peers)/2 {
		rf.mu.Lock()
		newTerm := rf.currentTerm
		rf.mu.Unlock()
		if currentTerm < newTerm {
			break
		}
		select {
		case reply := <-replyChans:
			if reply.Success {
				successCount++
			} else {
				failedCount++
			}
		case <-time.After(time.Duration(rf.heartBeatInterval) * time.Millisecond):
			failedCount++
			continue
		}
	}
	if successCount >= len(rf.peers)/2 {
	} else {
		rf.mu.Lock()
		rf.becomeFollower()
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartBeats() {
	for {
		if rf.killed() {
			log.Printf("%v is killed, break heartBeats", rf.me)
			return
		}
		rf.mu.Lock()
		// defer rf.mu.Unlock()
		if rf.isLeader {
			rf.lastHeartbeatTime = time.Now()
			rf.mu.Unlock()
			rf.sendHeartBeat()
		} else {
			rf.mu.Unlock()
			return
		}
		time.Sleep(time.Duration(rf.heartBeatInterval) * time.Millisecond)
	}
}

// election timeout check goroutine
// start when become follower
// will exit when it not follower or become candidate
func (rf *Raft) electionTimeoutCheck() {
	for {
		if rf.killed() {
			//
			log.Printf("%v is killed, break election timeout check", rf.me)
			return
		}
		rf.mu.Lock()
		if !rf.isLeader {
			currentTime := time.Now()
			if currentTime.Sub(rf.lastHeartbeatTime).Milliseconds() > int64(rf.leaderElectionTimeout) {
				rf.becomeCandidate()
				go rf.startRequestVote()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(rf.leaderElectionTimeout) * time.Millisecond)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
// the problems I face when finish 2A is the sleep method does not release lock
// defer may release or acquire lock wrong.
// so need to add some log to make sure loop is ongoing, not blocked by wrong lock.
// use chan for waiting request finish.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.heartBeatInterval = 100 + rand.Intn(50)
	rf.leaderElectionTimeout = 200 + rand.Intn(300)

	rf.isLeader = false
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = follower
	rf.lastHeartbeatTime = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	f, err := os.OpenFile("testlogfile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal("could not open logfile testlogfile")
	} else {
		log.SetOutput(f)
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	go rf.electionTimeoutCheck()
	return rf
}
