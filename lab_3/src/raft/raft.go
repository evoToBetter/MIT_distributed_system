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
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
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

func (le *LogEntry) String() string {
	return fmt.Sprintf("{Term: %v, Index: %v, Content: %v }", le.Term, le.Index, le.Content)
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
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	mu                          sync.Mutex          // Lock to protect shared access to this peer's state
	peers                       []*labrpc.ClientEnd // RPC end points of all peers
	persister                   *Persister          // Object to hold this peer's persisted state
	me                          int                 // this peer's index into peers[]
	dead                        int32               // set by Kill()
	currentTerm                 int                 // state, current term
	votedFor                    int                 // state, voted id in current term, -1 for nil.
	logEntries                  []*LogEntry         // log entry list
	commitIndex                 int                 // index of highest log entry known to be committed
	lastApplied                 int                 // index of highest log entry applied to state machine
	heartBeatInterval           int                 // heart beat interval, ms
	leaderElectionTimeout       int                 // leader election delay timeout, ms
	leaderElectionCheckInterval int                 // leader election check interval
	isLeader                    bool                // leader mark
	nextIndex                   []int               // leader state, next index for each server
	matchIndex                  []int               // leader state, index of highest log entry
	state                       electionState       // leader election state, follower, candidate, leader
	applyChan                   chan ApplyMsg       // chan to send apply message
	heartBeatIntervalBase       int                 // heart beat interval base time
	heartBeatIntervalRand       int                 // random time for heart beat
	electionTimeoutBase         int                 // election timeout base time
	electionTimeoutRand         int                 // random time for election timeout
	requestTotalTimeout         int64               // total time out limit for send request
	grantVote                   chan bool           // notify channel for grant vote
	appendEntry                 chan bool           // notify channel for append entry
	heartbeatFinish             chan bool           // notify channel for heartbeat success finished.
}

func (rf *Raft) becomeFollower() {
	log.Printf("%v become follower", rf.me)
	rf.state = follower
	rf.votedFor = -1
	rf.isLeader = false
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	log.Printf("%v become candidate", rf.me)
	rf.state = candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.isLeader = false
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	log.Printf("%v become leader, index %v", rf.me, rf.logEntries[len(rf.logEntries)-1].Index)
	rf.state = leader
	rf.votedFor = -1
	rf.isLeader = true
	newNextIndex := 0
	if rf.logEntries != nil && len(rf.logEntries) != 0 {
		newNextIndex = rf.logEntries[len(rf.logEntries)-1].Index + 1
	}
	for i := range rf.nextIndex {
		rf.nextIndex[i] = newNextIndex
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.persist()
	go rf.sendMessage(rf.grantVote)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	log.Printf("rf %v lock start, lock num %v", rf.me, 1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.isLeader
	// Your code here (2A).
	log.Printf("rf %v lock finish, lock num %v", rf.me, 1)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastApplied)
	e.Encode(rf.commitIndex)
	e.Encode(len(rf.logEntries))
	for _, entry := range rf.logEntries {
		e.Encode(&entry)
	}
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
	var currentTerm int
	var votedFor int
	var lastApplied int
	var commitIndex int
	var logEntriesLen int
	var logEntries []*LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&lastApplied) != nil || d.Decode(&commitIndex) != nil || d.Decode(&logEntriesLen) != nil {
		log.Printf("could not find currentTerm or votedFor or logEntriesLen from persister!")
	} else {
		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 2)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = lastApplied
		rf.commitIndex = commitIndex
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 2)
		for i := 0; i < logEntriesLen; i++ {
			var logEntry LogEntry
			if d.Decode(&logEntry) != nil {
				log.Printf("could not find logEntry from persister!")
			} else {
				logEntries = append(logEntries, &logEntry)
			}
		}
		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 3)
		rf.logEntries = logEntries
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 3)
	}
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
	Term         int
	Server       int
	LastLogIndex int //voter lastlogIndex
	LastLogTerm  int
	VoteGranted  bool
}

func (rf *Raft) startRequestVote() {
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 4)
	// change self votedFor
	rf.votedFor = rf.me
	votedCount := 0
	failedCount := 0
	currentTerm := rf.currentTerm
	replyChans := make(chan *RequestVoteReply)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		request := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
			LastLogIndex: rf.logEntries[len(rf.logEntries)-1].Index,
		}
		go rf.sendRequestVote(i, &request, replyChans)
	}
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 4)
	for votedCount+failedCount < len(rf.peers)-1 {
		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 5)
		nowTerm := rf.currentTerm
		state := rf.state
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 5)
		reply := <-replyChans

		log.Printf("raft %v Get vote reply: %v", rf.me, reply)
		if reply.VoteGranted {
			votedCount++
		} else {
			failedCount++
		}
		if state != candidate || currentTerm != nowTerm {
			log.Printf("rf %v do not meet the requirement, state %v, nowTerm %v, skip request vote check", rf.me, state, nowTerm)
			continue
		}

		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 6)
		log.Printf("raft %v Get vote reply after lock: %v", rf.me, reply)
		if reply.Term > rf.currentTerm {
			log.Printf("raft %v Get lager term vote reply %v", rf.me, reply)
			rf.currentTerm = reply.Term
			rf.becomeFollower()
		} else if votedCount >= len(rf.peers)/2 && rf.state == candidate && rf.currentTerm == currentTerm {
			rf.becomeLeader()
		}
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 6)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyChans chan *RequestVoteReply) bool {
	log.Printf("%v send vote request to %v with request %v", args.CandidateID, server, args)
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	if !ok {
		reply.VoteGranted = false
	}
	replyChans <- reply
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// var candidateLastIndex = args.lastLogIndex
	// var candidateLastLogTerm = args.lastLogTerm
	log.Printf("rf %v get vote request %v", rf.me, args)
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 7)
	defer rf.mu.Unlock()
	log.Printf("rf %v get vote request after lock %v", rf.me, args)
	var candidateTerm = args.Term
	var candidateId = args.CandidateID
	reply.Term = rf.currentTerm
	reply.Server = rf.me
	if candidateTerm > rf.currentTerm {
		rf.currentTerm = candidateTerm
		rf.becomeFollower()
	}
	if candidateTerm < rf.currentTerm {
		log.Printf("rf %v deny the vote request because term %v exceed request term %v, return vote request %v false", rf.me, rf.currentTerm, candidateTerm, args)
		reply.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
		reply.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		reply.VoteGranted = false
		log.Printf("rf %v lock finish, lock num %v", rf.me, 7)
		return
	} else if rf.votedFor != -1 && rf.votedFor != candidateId {
		log.Printf("rf %v has voted for %v, deny this vote request!", rf.me, rf.votedFor)
		reply.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
		reply.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		reply.VoteGranted = false
		log.Printf("rf %v lock finish, lock num %v", rf.me, 7)
		return
	} else {
		lastLogIndex := rf.logEntries[len(rf.logEntries)-1].Index
		lastLogTerm := rf.logEntries[lastLogIndex].Term
		if rf.votedFor == -1 || rf.votedFor == candidateId {
			if args.LastLogTerm > lastLogTerm {
				log.Printf("%v accept the vote request %v, return true", rf.me, args)
				rf.votedFor = candidateId
				rf.persist()
				reply.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
				reply.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
				reply.VoteGranted = true
				// reset election timer
				go rf.sendMessage(rf.grantVote)
				log.Printf("rf %v lock finish, lock num %v", rf.me, 7)
				return
			} else if args.LastLogTerm == lastLogTerm {
				if args.LastLogIndex >= lastLogIndex {
					log.Printf("%v accept the vote request %v, return true", rf.me, args)
					rf.votedFor = candidateId
					rf.persist()
					reply.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
					reply.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
					reply.VoteGranted = true
					// reset election timer
					go rf.sendMessage(rf.grantVote)
					log.Printf("rf %v lock finish, lock num %v", rf.me, 7)
					return
				}
			}
			log.Printf("%v deny the vote request because rf.commitIndex %v exceed request commitIndex %v, return false", rf.me, rf.commitIndex, args.LastLogIndex)
			reply.VoteGranted = false
			log.Printf("rf %v lock finish, lock num %v", rf.me, 7)
			return
		} else {
			log.Printf("%v deny the vote request because already votedFor %v, return false", rf.me, rf.votedFor)
			reply.VoteGranted = false
			log.Printf("rf %v lock finish, lock num %v", rf.me, 7)
			return
		}
	}
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
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 8)
	defer rf.mu.Unlock()
	if rf.isLeader {
		// save log to self own entries
		leaderLogEntry := LogEntry{
			Term:    rf.currentTerm,
			Index:   rf.nextIndex[rf.me],
			Content: command,
		}
		index = leaderLogEntry.Index
		term = leaderLogEntry.Term
		isLeader = true
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
		for i := range rf.nextIndex {
			rf.nextIndex[i]++
		}
		rf.logEntries = append(rf.logEntries, &leaderLogEntry)
		rf.persist()
		log.Printf("%v start with term %v, index %v", rf.me, rf.currentTerm, rf.nextIndex[rf.me])
		// log.Printf("%v start with command %v, with term %v, index %v, logEntries %v", rf.me, command, rf.currentTerm, rf.nextIndex[rf.me], len(rf.logEntries))
		// log.Printf("rf %v current state: term %v, votedFor %v, logEntries %v, ", rf.me, rf.currentTerm, rf.votedFor, len(rf.logEntries))
		go rf.notifyNewLogEntry(&leaderLogEntry)
		isLeader = true
	} else {
		isLeader = false
	}
	log.Printf("rf %v lock finish, lock num %v", rf.me, 8)
	return index, term, isLeader
}

func (rf *Raft) notifyNewLogEntry(logEntry *LogEntry) {
	appendRequests := make([]*RequestAppendEntriesArgs, len(rf.peers))
	replyChans := make(chan *RequestAppendEntriesReply)
	newIndex := logEntry.Index
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 9)
	for i := range rf.peers {
		if i == rf.me {
			continue
		} else {
			transIndex := 0
			endIndex := logEntry.Index + 1
			transIndex = rf.matchIndex[i] + 1
			var entries []*LogEntry
			if transIndex <= logEntry.Index {
				entries = rf.logEntries[transIndex:endIndex]
			} else {
				entries = rf.logEntries[endIndex:endIndex]
			}

			prevLogIndex := rf.matchIndex[i]
			if prevLogIndex == len(rf.logEntries) {
				prevLogIndex = prevLogIndex - 1
			}
			prevLogTerm := rf.logEntries[prevLogIndex].Term
			request := RequestAppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			appendRequests[i] = &request
			go rf.appendEntries(i, &request, replyChans)
		}
	}
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 9)
	log.Printf("rf %v start append entries %v", rf.me, logEntry)
	successCount := 0
	failCount := 0
	logTransRecord := make([]bool, len(rf.peers))
	logTransRecord[rf.me] = true
	for successCount+failCount < len(rf.peers)-1 {
		appendReply := <-replyChans

		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 10)
		log.Printf("rf %v get append entries reply %v", rf.me, appendReply)
		if appendReply.Term > rf.currentTerm {
			rf.currentTerm = appendReply.Term
			rf.becomeFollower()
		}
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 10)
		if appendReply.Success {
			successCount++
			logTransRecord[appendReply.Server] = true

			rf.mu.Lock()
			log.Printf("rf %v lock start, lock num %v", rf.me, 11)
			if rf.matchIndex[appendReply.Server] < newIndex {
				rf.matchIndex[appendReply.Server] = newIndex
			}
			rf.mu.Unlock()
			log.Printf("rf %v lock finish, lock num %v", rf.me, 11)
		} else {
			failCount++
		}
		if successCount >= len(rf.peers)/2 {
			log.Printf("rf %v log %v is appended by major raft node", rf.me, logEntry.Index)
			go rf.commitLog(logEntry.Index, logTransRecord)
		}
	}
	log.Printf("rf %v start append entries %v, success %v, failed %v", rf.me, logEntry, successCount, failCount)
	if failCount != 0 && !rf.killed() {
		for i, success := range logTransRecord {
			if !success {
				if len(appendRequests[i].Entries) > 0 {
					go rf.appendIndexIndefinitely(i, appendRequests[i])
				}
			}
		}
	}
}

func (rf *Raft) appendIndexIndefinitely(server int, request *RequestAppendEntriesArgs) {
	log.Printf("raft %v send append entry %v to %v indefinitely!", rf.me, request.Entries[0].Index, server)
	replyChan := make(chan *RequestAppendEntriesReply)
	success := false
	for {

		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 12)
		isLeader := rf.isLeader
		lastLogIndex := rf.matchIndex[server]
		syncTerm := rf.currentTerm
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 12)
		if !isLeader || success || rf.killed() {
			log.Printf("raft %v stop to send append entry %v to %v indefinitely!", rf.me, request.Entries[0].Index, server)
			break
		}
		if len(request.Entries) > 0 && lastLogIndex >= request.Entries[len(request.Entries)-1].Index {
			log.Printf("%v log has catch up, no need to send log entries!", server)
			break
		}
		go rf.appendEntries(server, request, replyChan)

		reply := <-replyChan

		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 13)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.becomeFollower()
		}
		isLeader = rf.isLeader
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 13)
		if !isLeader || rf.killed() || currentTerm != syncTerm {
			log.Printf("raft %v stop to send append entry %v to %v indefinitely!", rf.me, request.Entries[0].Index, server)
			break
		}
		if reply.Success {

			rf.mu.Lock()
			log.Printf("rf %v lock start, lock num %v", rf.me, 14)
			if rf.nextIndex[reply.Server] <= reply.FinishedIndex {
				rf.nextIndex[reply.Server] = reply.FinishedIndex + 1
			}
			if rf.matchIndex[reply.Server] < reply.FinishedIndex {
				rf.matchIndex[reply.Server] = reply.FinishedIndex
			}
			success = true
			rf.mu.Unlock()
			log.Printf("rf %v lock finish, lock num %v", rf.me, 14)
		} else {
			success = false
		}
	}

	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 15)
	isleader := rf.isLeader
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 15)
	if success && isleader {
		commitRequest := CommitLogRequest{
			Index: request.Entries[len(request.Entries)-1].Index,
		}
		go rf.sendCommitMsg(server, &commitRequest)
	}
}

func (rf *Raft) commitLog(index int, logTransRecord []bool) {
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 16)
	if rf.commitIndex < index && rf.isLeader {
		rf.applyMsgHandler(index)
		rf.commitIndex = index
		rf.persist()
		for i := range logTransRecord {
			if i == rf.me || !logTransRecord[i] {
				continue
			}
			request := CommitLogRequest{
				Index: index,
			}
			go rf.sendCommitMsg(i, &request)
		}
	}
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 16)
}

type CommitLogRequest struct {
	Index int
}

type CommitLogResp struct {
	Server  int
	Success bool
}

func (rf *Raft) sendCommitMsg(serverNum int, request *CommitLogRequest) {
	success := false
	replyChan := make(chan *CommitLogResp)
	for {

		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 17)
		isLeader := rf.isLeader
		appliedIndex := rf.matchIndex[serverNum]
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 17)
		if success || !isLeader || rf.killed() || appliedIndex > request.Index {
			break
		}

		go rf.sendCommitMsgRequest(serverNum, request, replyChan)
		rsp := <-replyChan

		if rsp.Success {
			success = true
		}
	}
}

func (rf *Raft) sendCommitMsgRequest(server int, request *CommitLogRequest, replyChan chan *CommitLogResp) bool {
	resp := CommitLogResp{}
	ok := rf.peers[server].Call("Raft.CommitMsgHandler", request, &resp)
	if !ok {
		resp.Success = false
	}
	replyChan <- &resp
	return ok
}

func (rf *Raft) CommitMsgHandler(request *CommitLogRequest, resp *CommitLogResp) {
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 18)
	if rf.commitIndex < request.Index {
		lastLogIndex := rf.logEntries[len(rf.logEntries)-1].Index
		if lastLogIndex < request.Index {
			resp.Server = rf.me
			resp.Success = false
		} else {
			rf.applyMsgHandler(request.Index)
			rf.commitIndex = request.Index
			rf.persist()
			resp.Server = rf.me
			resp.Success = true
		}
	} else {
		resp.Server = rf.me
		resp.Success = true
	}
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 18)
}

func (rf *Raft) applyMsgHandler(latestAppliedId int) {
	for rf.lastApplied < latestAppliedId && !rf.killed() {
		prevIndex := rf.lastApplied + 1
		log.Printf("rf %v start apply index %v", rf.me, prevIndex)
		command := rf.logEntries[prevIndex].Content
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: prevIndex,
		}
		log.Printf("rf %v, apply msg %v", rf.me, applyMsg.CommandIndex)
		rf.applyChan <- applyMsg
		rf.lastApplied++
	}
}

type RequestAppendEntriesArgs struct {
	Term             int
	LeaderId         int
	PrevLogIndex     int
	PrevLogTerm      int
	PrevAppliedIndex int
	Entries          []*LogEntry
	LeaderCommit     int
}

type RequestAppendEntriesReply struct {
	Server          int
	Term            int
	PrevLogIndex    int
	PrevLogTerm     int
	FinishedIndex   int
	NeedSyncEntries bool
	Success         bool
}

// handle appendEntries request
func (rf *Raft) RequestAppendEntriesHandler(request *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// lock
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 19)
	defer rf.mu.Unlock()
	log.Printf("%v at Term %v get apeend log request %v", rf.me, rf.currentTerm, request)
	reply.Server = rf.me
	if request.Term > rf.currentTerm {
		log.Printf("rf %v term %v smaller than %v!", rf.me, rf.currentTerm, request)
		rf.currentTerm = request.Term
		rf.becomeFollower()
	} else if request.Term < rf.currentTerm {
		log.Printf("rf %v term larger than request term %v", rf.currentTerm, request.Term)
		reply.Term = rf.currentTerm
		log.Printf("rf %v lock finish, lock num %v", rf.me, 19)
		return
	}
	if request.Entries == nil || len(request.Entries) == 0 {

		log.Printf("%v at Term %v get heartbeat", rf.me, rf.currentTerm)
		// heartbeat request
		if len(rf.logEntries) <= request.PrevLogIndex {
			log.Printf("log entries for %v is not sync with leader, entries: %v", rf.me, len(rf.logEntries))
			reply.PrevLogIndex = len(rf.logEntries) - 1
			reply.PrevLogTerm = rf.logEntries[reply.PrevLogIndex].Term
			reply.NeedSyncEntries = true
		} else if rf.logEntries[request.PrevLogIndex].Term != request.PrevLogTerm {
			log.Printf("log entries for %v is not sync with leader, entries: %v", rf.me, len(rf.logEntries))
			reply.PrevLogIndex = request.PrevLogIndex - 1
			reply.PrevLogTerm = rf.logEntries[reply.PrevLogIndex].Term
			reply.NeedSyncEntries = true
		} else {
			if rf.lastApplied < request.PrevAppliedIndex {
				log.Printf("%v get applied index %v from request exceed self applied index %v", rf.me, request.PrevAppliedIndex, rf.lastApplied)
				rf.applyMsgHandler(request.PrevAppliedIndex)
				rf.commitIndex = request.PrevAppliedIndex
				rf.persist()
			}
		}
		reply.Success = true
		//reset election timer
		go rf.sendMessage(rf.appendEntry)
		rf.currentTerm = request.Term
		log.Printf("%v get heartbeat success!", rf.me)
		rf.becomeFollower()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 19)
		return
	} else {
		// none heartbeat request
		// log.Printf("%v at Term %v get append request rf.logEntries: %v", rf.me, rf.currentTerm, request)
		// if rf.state != follower {
		// 	log.Printf("%v is not follower return false, state: %v", rf.me, rf.state)
		// 	reply.Term = rf.currentTerm
		// 	reply.PrevLogIndex = 0
		// 	reply.PrevLogTerm = -1
		// 	reply.Success = false
		// 	return
		// }
		// term check
		if request.Term < rf.currentTerm {
			log.Printf("%v term %v exceed request term %v, return false", rf.me, rf.currentTerm, request.Term)
			reply.Term = rf.currentTerm
			reply.PrevLogIndex = 0
			reply.PrevLogTerm = -1
			reply.Success = false
			log.Printf("rf %v lock finish, lock num %v", rf.me, 19)
			return
		}
		// log check
		if rf.logEntries != nil && len(rf.logEntries) > 0 {
			if len(rf.logEntries) <= request.PrevLogIndex {
				log.Printf("%v term %v exceed request term %v, return false", rf.me, rf.currentTerm, request.Term)
				reply.Term = rf.currentTerm
				reply.PrevLogIndex = len(rf.logEntries) - 1
				reply.PrevLogTerm = rf.logEntries[reply.PrevLogIndex].Term
				reply.Success = false
				log.Printf("rf %v lock finish, lock num %v", rf.me, 19)
				return
			}
			if rf.logEntries[request.PrevLogIndex].Term != request.PrevLogTerm {
				log.Printf("%v term %v no equal term %v, return false", rf.me, rf.logEntries[request.PrevLogIndex].Term, request.PrevLogTerm)
				reply.Term = rf.currentTerm
				reply.PrevLogIndex = request.PrevLogIndex - 1
				reply.PrevLogTerm = rf.logEntries[reply.PrevLogIndex].Term
				reply.Success = false
				log.Printf("rf %v lock finish, lock num %v", rf.me, 19)
				return
			}
		}
		requestEntries := request.Entries
		startIndex := 0
		if rf.logEntries != nil && len(rf.logEntries) > 0 {
			for i := range requestEntries {
				entry := requestEntries[i]
				if entry.Index < len(rf.logEntries) {
					localEntry := rf.logEntries[entry.Index]
					if entry.Term != localEntry.Term {
						rf.logEntries = rf.logEntries[0:entry.Index]
						break
					}
				} else {
					break
				}
				startIndex++
			}
		}
		requestEntries = requestEntries[startIndex:]
		rf.logEntries = append(rf.logEntries, requestEntries...)
		log.Printf("%v success append entries: %v, last index %v", rf.me, len(rf.logEntries), rf.logEntries[len(rf.logEntries)-1].Index)
		rf.currentTerm = request.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		reply.FinishedIndex = rf.logEntries[len(rf.logEntries)-1].Index
		// reset election timer
		go rf.sendMessage(rf.appendEntry)
		rf.persist()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 19)
		return
	}
}

func (rf *Raft) logEntriesSync(server int, prevIndex int, prevTerm int) {

	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 20)
	log.Printf("%v sync log entries: prevIndex %v, prevLogTerm %v", server, prevIndex, prevTerm)
	matchedIndex := len(rf.logEntries) - 1
	syncTerm := rf.currentTerm
	if rf.logEntries[prevIndex].Term != prevTerm && prevIndex > 0 {
		prevIndex = prevIndex - 1
	}
	logEntries := rf.logEntries[prevIndex:len(rf.logEntries)]
	request := RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.logEntries[prevIndex].Term,
		Entries:      logEntries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 20)
	replyChan := make(chan *RequestAppendEntriesReply)
	go rf.appendEntries(server, &request, replyChan)
	reply := <-replyChan
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 21)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.becomeFollower()
	}
	isLeader := rf.isLeader
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 21)
	if !isLeader || rf.killed() || syncTerm != currentTerm {
		return
	}
	if reply.Success {
		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 22)
		if rf.matchIndex[server] < matchedIndex {
			rf.matchIndex[server] = matchedIndex
		}
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 22)
		log.Printf("%v success sync log entries", server)
	} else {
		go rf.logEntriesSync(server, reply.PrevLogIndex, reply.PrevLogTerm)
	}

	// if syncSuccess {
	// 	request := CommitLogRequest{
	// 		Index: matchedIndex,
	// 	}
	// 	go rf.sendCommitMsg(server, &request)
	// }
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 23)
	currentTerm := rf.currentTerm
	log.Printf("leader %v at term %v start heartbeat!", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 23)
	replyChans := make(chan *RequestAppendEntriesReply)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 24)
		prevLogIndex := len(rf.logEntries) - 1
		heartBeatRequest := RequestAppendEntriesArgs{
			Term:             rf.currentTerm,
			LeaderId:         rf.me,
			PrevLogIndex:     prevLogIndex,
			PrevLogTerm:      rf.logEntries[prevLogIndex].Term,
			PrevAppliedIndex: rf.lastApplied,
			Entries:          nil,
			LeaderCommit:     rf.commitIndex,
		}
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 24)
		go rf.appendEntries(i, &heartBeatRequest, replyChans)
	}
	// todo
	// what we could do if cloud not get response from followers.
	// time.Sleep(time.Duration(rf.heartBeatInterval) * time.Millisecond)
	successCount := 0
	failedCount := 0
	log.Printf("rf %v wait for leader heart beat reply", rf.me)
	for successCount+failedCount < len(rf.peers)-1 {
		reply := <-replyChans
		log.Printf("rf %v get heart beat reply %v", rf.me, reply)

		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 25)
		log.Printf("rf %v check term %v", rf.me, reply.Term)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.becomeFollower()
		}
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 25)
		if reply.Success {
			successCount++
			if reply.NeedSyncEntries {
				log.Printf("follower %v need to catch up the log entries with prevIndex %v", reply.Server, reply.PrevLogIndex)
				go rf.logEntriesSync(reply.Server, reply.PrevLogIndex, reply.PrevLogTerm)
			}
		} else {
			failedCount++
		}
		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 26)
		newTerm := rf.currentTerm
		isLeader := rf.isLeader
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 26)
		if currentTerm < newTerm || !isLeader {
			log.Printf("rf %v state not meet the request, term %v, is leader %v, heart beat check skip", rf.me, newTerm, isLeader)
			continue
		}
		if successCount >= len(rf.peers)/2 {
			log.Printf("leader %v get heartbeat success", rf.me)
			continue
		}

		if failedCount > len(rf.peers)/2 {
			rf.mu.Lock()
			log.Printf("rf %v lock start, lock num %v", rf.me, 27)
			log.Printf("leader %v did not get heartbeat success, become follower", rf.me)
			rf.resetElectionTimeout(rf.electionTimeoutBase, rf.electionTimeoutRand)
			rf.becomeFollower()
			rf.mu.Unlock()
			log.Printf("rf %v lock finish, lock num %v", rf.me, 27)
		}
	}
	log.Printf("rf %v heart beat finished", rf.me)
}

func (rf *Raft) heartBeats() {
	rf.mu.Lock()
	log.Printf("rf %v lock start, lock num %v", rf.me, 28)
	isLeader := rf.isLeader
	rf.mu.Unlock()
	log.Printf("rf %v lock finish, lock num %v", rf.me, 28)
	if rf.killed() {
		log.Printf("%v is killed, break heartBeats", rf.me)
		return
	}
	if isLeader {
		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 29)
		log.Printf("%v at %v is leader start heartbeat now", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 29)
		rf.sendHeartBeat()
	} else {
		return
	}
}

func (rf *Raft) appendEntries(server int, request *RequestAppendEntriesArgs, replyChans chan *RequestAppendEntriesReply) bool {
	reply := &RequestAppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.RequestAppendEntriesHandler", request, reply)
	if !ok {
		reply.Success = false
	}
	replyChans <- reply
	return ok
}

func (rf *Raft) sendMessage(channel chan bool) {
	log.Printf("rf %v send timer message start.", rf.me)
	select {
	case <-channel:
	default:
	}
	channel <- true
	log.Printf("rf %v send timer message finish.", rf.me)
}

// election timeout check goroutine
// start when become follower
// will exit when it not follower or become candidate
func (rf *Raft) electionTimer() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		log.Printf("rf %v lock start, lock num %v", rf.me, 30)
		log.Printf("rf %v start another election timer!", rf.me)
		state := rf.state
		rf.resetElectionTimeout(rf.electionTimeoutBase, rf.electionTimeoutRand)
		electionTimeout := rf.leaderElectionTimeout
		rf.mu.Unlock()
		log.Printf("rf %v lock finish, lock num %v", rf.me, 30)
		switch state {
		case follower, candidate:
			log.Printf("rf %v start follower/candidate election timer! %v", rf.me, state)
			select {
			case <-rf.grantVote:
			case <-rf.appendEntry:
			case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
				rf.mu.Lock()
				log.Printf("rf %v lock start, lock num %v", rf.me, 31)
				rf.becomeCandidate()
				rf.mu.Unlock()
				log.Printf("rf %v lock finish, lock num %v", rf.me, 31)
				go rf.startRequestVote()
			}
		case leader:
			log.Printf("rf %v start leader heartbeat timer! %v", rf.me, state)
			go rf.heartBeats()
			time.Sleep(time.Duration(rf.heartBeatInterval) * time.Millisecond)
		}
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

func (rf *Raft) resetElectionTimeout(base int, additional int) {
	rf.leaderElectionTimeout = base + rand.Intn(additional)
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
// RPC args and reply field must be start with capital letter.
// use chan for waiting request finish.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh
	rf.grantVote = make(chan bool)
	rf.appendEntry = make(chan bool)
	rf.heartbeatFinish = make(chan bool)
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	// heartBeat interval not exceed than 150ms, election timeout not exceed 500 but larger one heartbeat interval
	rf.heartBeatIntervalBase = 100
	rf.heartBeatIntervalRand = 50
	rf.heartBeatInterval = rf.heartBeatIntervalBase
	// rf.leaderElectionTimeout = 200 + rand.Intn(300)
	rf.electionTimeoutBase = 200
	rf.electionTimeoutRand = 300
	rf.resetElectionTimeout(rf.electionTimeoutBase, rf.electionTimeoutRand)
	rf.leaderElectionCheckInterval = rf.leaderElectionTimeout
	rf.isLeader = false
	rf.requestTotalTimeout = 30
	// init leader's state
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = follower
	// log entry index should start from 1 according to test
	rf.logEntries = make([]*LogEntry, 1)
	logEntry := LogEntry{
		Term:  -1,
		Index: 0,
	}
	rf.logEntries[0] = &logEntry
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// redirect log to file
	f, err := os.OpenFile("raft.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("could not open logfile raft.log")
	} else {
		log.SetOutput(f)
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	// disable log
	// log.SetFlags(0)
	// log.SetOutput(ioutil.Discard)
	// add point about raft start, report state point
	log.Printf("raft %v instance make finish, current term %v, logEntires %v", rf.me, rf.currentTerm, rf.logEntries)
	// start election time out check
	go rf.electionTimer()

	return rf
}
