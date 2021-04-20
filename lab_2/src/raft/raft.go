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
	applyChan             chan ApplyMsg       // chan to send apply message
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
	rf.lastHeartbeatTime = time.Now()
	rf.currentTerm++
	rf.isLeader = false
	rf.resetElectionTimeout(200, 300)
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	log.Printf("%v become leader, lastheartbeat %v", rf.me, rf.lastHeartbeatTime)
	rf.state = leader
	rf.votedFor = -1
	rf.isLeader = true
	rf.lastHeartbeatTime = time.Now()
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
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = lastApplied
		rf.commitIndex = commitIndex
		rf.mu.Unlock()
		for i := 0; i < logEntriesLen; i++ {
			var logEntry LogEntry
			if d.Decode(&logEntry) != nil {
				log.Printf("could not find logEntry from persister!")
			} else {
				logEntries = append(logEntries, &logEntry)
			}
		}
		rf.mu.Lock()
		rf.logEntries = logEntries
		rf.mu.Unlock()
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
	// change self votedFor
	rf.votedFor = rf.me
	votedCount := 0
	failedCount := 0
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
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &request, &reply, replyChans)
	}
	rf.mu.Unlock()
	maxReplyTerm := rf.currentTerm
	currentTerm := rf.currentTerm
	for votedCount < len(rf.peers)/2 && failedCount < len(rf.peers)/2 && !rf.killed() {
		rf.mu.Lock()
		nowTerm := rf.currentTerm
		rf.mu.Unlock()
		if currentTerm != nowTerm {
			break
		}
		select {
		case reply := <-replyChans:
			log.Printf("raft %v Get vote reply: %v", rf.me, reply)
			if reply.VoteGranted {
				votedCount++
			} else {
				if reply.Term > maxReplyTerm {
					maxReplyTerm = reply.Term
				}
				failedCount++
			}
		case <-time.After(time.Duration(rf.heartBeatInterval) * time.Millisecond):
			failedCount++
			continue
		}
	}
	rf.mu.Lock()
	if votedCount >= len(rf.peers)/2 && !rf.killed() {
		if rf.state == candidate {
			log.Printf("%v get vote count %v,  meet the requirement, become leader", rf.me, votedCount)
			rf.becomeLeader()
		} else {
			log.Printf("already get a leader, do nothing for vote success.")
		}
	} else {
		log.Printf("%v get vote not meet the requirement, become follower", rf.me)
		if !rf.isLeader && rf.currentTerm == currentTerm {
			if maxReplyTerm > rf.currentTerm {
				rf.currentTerm = maxReplyTerm
			}
			rf.becomeFollower()
		}
	}
	rf.mu.Unlock()
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
	log.Printf("%v send vote request to %v", args.CandidateID, reply.Server)
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var candidateTerm = args.Term
	var candidateId = args.CandidateID
	reply.Term = rf.currentTerm
	reply.Server = rf.me
	if rf.isLeader {
		log.Printf("%v is leader, return vote request false", rf.me)
		reply.VoteGranted = false
		return
	}
	if candidateTerm <= rf.currentTerm {
		log.Printf("%v term %v exceed request term %v, return vote request false", rf.me, rf.currentTerm, candidateTerm)
		reply.VoteGranted = false
		return
	} else {
		lastLogIndex := rf.logEntries[len(rf.logEntries)-1].Index
		lastLogTerm := rf.logEntries[lastLogIndex].Term
		if rf.votedFor == -1 || rf.votedFor == candidateId {
			if args.LastLogTerm > lastLogTerm {
				log.Printf("%v accept the vote request %v, return true", rf.me, args)
				rf.votedFor = candidateId
				reply.LastLogIndex = len(rf.logEntries) - 1
				rf.persist()
				reply.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
				reply.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
				reply.VoteGranted = true
				return
			} else if args.LastLogTerm == lastLogTerm {
				if args.LastLogIndex >= lastLogIndex {
					log.Printf("%v accept the vote request %v, return true", rf.me, args)
					rf.votedFor = candidateId
					reply.LastLogIndex = len(rf.logEntries) - 1
					rf.persist()
					reply.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
					reply.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
					reply.VoteGranted = true
					return
				}
			}
			log.Printf("%v deny the vote request because rf.commitIndex %v exceed request commitIndex %v, return false", rf.me, rf.commitIndex, args.LastLogIndex)
			reply.VoteGranted = false
			return
		} else {
			log.Printf("%v deny the vote request because already votedFor %v, return false", rf.me, rf.votedFor)
			reply.VoteGranted = false
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
		log.Printf("%v start with command %v, with term %v, index %v, logEntries %v", rf.me, command, rf.currentTerm, rf.nextIndex[rf.me], len(rf.logEntries))
		log.Printf("rf %v current state: term %v, votedFor %v, logEntries %v, ", rf.me, rf.currentTerm, rf.votedFor, len(rf.logEntries))
		go rf.notifyNewLogEntry(&leaderLogEntry)
		isLeader = true
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) notifyNewLogEntry(logEntry *LogEntry) {
	appendRequests := make([]*RequestAppendEntriesArgs, len(rf.peers))
	replyChans := make(chan *RequestAppendEntriesReply)
	newIndex := logEntry.Index
	rf.mu.Lock()
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
			go rf.AppendEntries(i, &request, replyChans)
		}
	}
	rf.mu.Unlock()

	successCount := 0
	failCount := 0
	logTransRecord := make([]bool, len(rf.peers))
	logTransRecord[rf.me] = true
	for successCount < len(rf.peers)/2 {
		rf.mu.Lock()
		isLeader := rf.isLeader
		rf.mu.Unlock()
		// if rf is not leader, give up to send notify
		if !isLeader || rf.killed() {
			break
		}
		requestTimeout := false
		for successCount < len(rf.peers)/2 && successCount+failCount < len(rf.peers)-1 && !requestTimeout {
			rf.mu.Lock()
			isLeader := rf.isLeader
			rf.mu.Unlock()
			// if rf is not leader, give up to send notify
			if !isLeader {
				break
			}
			select {
			case appendReply := <-replyChans:
				if appendReply.Success {
					successCount++
					logTransRecord[appendReply.Server] = true
					rf.mu.Lock()
					if rf.matchIndex[appendReply.Server] < newIndex {
						rf.matchIndex[appendReply.Server] = newIndex
					}
					rf.mu.Unlock()
				} else {
					failCount++
				}
			// timeout could not be very long, which will make config.one timeout.
			// need to fail the wait in a short time for test.TestUnreliableAgree2C.
			case <-time.After(time.Duration(rf.heartBeatInterval/2) * time.Millisecond):
				requestTimeout = true
			}
		}
		if successCount >= len(rf.peers)/2 {
			log.Printf("log %v is appended by major raft node", logEntry)
			go rf.commitLog(logEntry.Index, logTransRecord)
		} else {
			failCount = 0
			for i, success := range logTransRecord {
				if !success && len(appendRequests[i].Entries) > 0 {
					go rf.AppendEntries(i, appendRequests[i], replyChans)
				}
			}
			continue
		}
	}

	if failCount != 0 && !rf.killed() {
		for i, success := range logTransRecord {
			if !success {
				rf.mu.Lock()
				log.Printf("leader %v send append index indefinitely", rf.me)
				rf.mu.Unlock()
				if len(appendRequests[i].Entries) > 0 {
					go rf.appendIndexIndefinitely(i, appendRequests[i])
				}
			}
		}
	}
}

func (rf *Raft) appendIndexIndefinitely(server int, request *RequestAppendEntriesArgs) {
	log.Printf("raft %v send append entry %v to %v indifinitely!", rf.me, request.Entries, server)
	replyChan := make(chan *RequestAppendEntriesReply)
	success := false
	for {
		rf.mu.Lock()
		isLeader := rf.isLeader
		lastLogIndex := rf.matchIndex[server]
		rf.mu.Unlock()
		if !isLeader || success || rf.killed() {
			break
		}
		if len(request.Entries) > 0 && lastLogIndex >= request.Entries[len(request.Entries)-1].Index {
			log.Printf("%v log has catch up, no need to send log entries!", server)
			break
		}
		go rf.AppendEntries(server, request, replyChan)
		select {
		case reply := <-replyChan:
			if reply.Success {
				rf.mu.Lock()
				rf.nextIndex[reply.Server] += reply.AppendLogLen
				rf.matchIndex[reply.Server] += reply.AppendLogLen
				success = true
				rf.mu.Unlock()
			}
		case <-time.After(time.Duration(rf.heartBeatInterval) * time.Millisecond):
			log.Printf("send append entry %v to %v indifinitely timeout!", request.Entries, server)
		}
	}

	if success {
		commitRequest := CommitLogRequest{
			Index: request.Entries[len(request.Entries)-1].Index,
		}
		go rf.sendCommitMsg(server, &commitRequest)
	}
}

func (rf *Raft) commitLog(index int, logTransRecord []bool) {
	rf.mu.Lock()
	if rf.commitIndex < index {
		rf.applyMsgHandler(index)
		rf.commitIndex = index
		rf.persist()
		for i := range logTransRecord {
			if logTransRecord[i] {
				if i == rf.me {
					continue
				}
				request := CommitLogRequest{
					Index: index,
				}
				go rf.sendCommitMsg(i, &request)
			}
		}
	}
	rf.mu.Unlock()
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
		isLeader := rf.isLeader
		appliedIndex := rf.matchIndex[serverNum]
		rf.mu.Unlock()
		if success || !isLeader || rf.killed() || appliedIndex > request.Index {
			break
		}
		rf.mu.Lock()
		log.Printf("%v send commit msg request %v", rf.me, request)
		rf.mu.Unlock()
		go rf.sendCommitMsgRequest(serverNum, request, replyChan)
		select {
		case rsp := <-replyChan:
			if rsp.Success {
				success = true
			}
		case <-time.After(time.Duration(rf.heartBeatInterval) * time.Millisecond):
			success = false
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
}

func (rf *Raft) applyMsgHandler(latestAppliedId int) {
	for rf.lastApplied < latestAppliedId && !rf.killed() {
		prevIndex := rf.lastApplied + 1
		log.Printf("%v start apply index %v", rf.me, prevIndex)
		command := rf.logEntries[prevIndex].Content
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: prevIndex,
		}
		log.Printf("raft %v, apply msg %v", rf.me, applyMsg)
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
	AppendLogLen    int
	NeedSyncEntries bool
	Success         bool
}

// handle appendEntries request
func (rf *Raft) RequestAppendEntriesHandler(request *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("%v at Term %v get apeend log request %v", rf.me, rf.currentTerm, request)
	reply.Server = rf.me
	if request.Entries == nil || len(request.Entries) == 0 {
		log.Printf("%v at Term %v get heartbeat", rf.me, rf.currentTerm)
		// heartbeat request
		if rf.isLeader {
			log.Printf("%v is leader, so return heart beat false", rf.me)
			reply.Term = rf.currentTerm
			reply.PrevLogIndex = 0
			reply.PrevLogTerm = -1
			reply.Success = false
			return
		}
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
		rf.lastHeartbeatTime = time.Now()
		rf.currentTerm = request.Term
		log.Printf("%v get heartbeat success!", rf.me)
		rf.becomeFollower()
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
				return
			}
			if rf.logEntries[request.PrevLogIndex].Term != request.PrevLogTerm {
				log.Printf("%v term %v no equal term %v, return false", rf.me, rf.currentTerm, request.Term)
				reply.Term = rf.currentTerm
				reply.PrevLogIndex = request.PrevLogIndex - 1
				reply.PrevLogTerm = rf.logEntries[reply.PrevLogIndex].Term
				reply.Success = false
				return
			}
		}
		rf.lastHeartbeatTime = time.Now()
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
		log.Printf("%v success append entries: %v, %v", rf.me, requestEntries, len(rf.logEntries))
		rf.currentTerm = request.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		reply.AppendLogLen = len(requestEntries)
		rf.persist()
		return
	}
}

func (rf *Raft) logEntriesSync(server int, prevIndex int, prevTerm int) {
	rf.mu.Lock()
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
	replyChan := make(chan *RequestAppendEntriesReply)
	go rf.AppendEntries(server, &request, replyChan)
	select {
	case reply := <-replyChan:
		rf.mu.Lock()
		isLeader := rf.isLeader
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		if !isLeader || rf.killed() || syncTerm != currentTerm {
			break
		}
		if reply.Success {
			rf.mu.Lock()
			if rf.matchIndex[server] < matchedIndex {
				rf.matchIndex[server] = matchedIndex
			}
			rf.mu.Unlock()
			log.Printf("%v success sync log entries", server)
		} else {
			go rf.logEntriesSync(server, reply.PrevLogIndex, reply.PrevLogTerm)
		}
	case <-time.After(time.Duration(rf.heartBeatInterval) * time.Millisecond):
		log.Printf("timeout to append entries to make follower catch up.")
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
	currentTerm := rf.currentTerm
	log.Printf("leader %v at term %v start heartbeat!", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	replyChans := make(chan *RequestAppendEntriesReply)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
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
		go rf.AppendEntries(i, &heartBeatRequest, replyChans)
	}
	// todo
	// what we could do if cloud not get response from followers.
	// time.Sleep(time.Duration(rf.heartBeatInterval) * time.Millisecond)
	successCount := 0
	failedCount := 0
	for successCount < len(rf.peers)/2 && failedCount < len(rf.peers)/2 && !rf.killed() {
		rf.mu.Lock()
		newTerm := rf.currentTerm
		isLeader := rf.isLeader
		rf.mu.Unlock()
		if currentTerm < newTerm || !isLeader {
			break
		}
		select {
		case reply := <-replyChans:
			if reply.Success {
				successCount++
				if reply.NeedSyncEntries {
					log.Printf("follower %v need to catch up the log entries with prevIndex %v", reply.Server, reply.PrevLogIndex)
					go rf.logEntriesSync(reply.Server, reply.PrevLogIndex, reply.PrevLogTerm)
				}
			} else {
				failedCount++
			}
		case <-time.After(time.Duration(rf.heartBeatInterval/(len(rf.peers)-1)) * time.Millisecond):
			failedCount++
			continue
		}
	}
	if successCount >= len(rf.peers)/2 {
		rf.mu.Lock()
		log.Printf("rf %v heartbeat success", rf.me)
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		log.Printf("leader %v did not get heartbeat success, become follower", rf.me)
		rf.becomeFollower()
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartBeats() {
	for {
		rf.mu.Lock()
		isLeader := rf.isLeader
		rf.mu.Unlock()
		if rf.killed() {
			rf.mu.Lock()
			log.Printf("%v is killed, break heartBeats", rf.me)
			rf.mu.Unlock()
			return
		}
		if isLeader {
			rf.mu.Lock()
			rf.lastHeartbeatTime = time.Now()
			log.Printf("%v at %v is leader start heartbeat now %v", rf.me, rf.currentTerm, rf.lastHeartbeatTime.UnixNano()/int64(time.Millisecond))
			rf.mu.Unlock()
			rf.sendHeartBeat()
		} else {
			return
		}
		time.Sleep(time.Duration(rf.heartBeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) AppendEntries(server int, request *RequestAppendEntriesArgs, replyChans chan *RequestAppendEntriesReply) bool {
	reply := RequestAppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.RequestAppendEntriesHandler", request, &reply)
	if !ok {
		reply.Success = false
	}
	replyChans <- &reply
	return ok
}

// election timeout check goroutine
// start when become follower
// will exit when it not follower or become candidate
func (rf *Raft) electionTimeoutCheck() {
	for {
		if rf.killed() {
			// report state point
			rf.mu.Lock()
			log.Printf("%v is killed, break election timeout check", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Lock()
		if rf.state == follower {
			currentTime := time.Now()
			if currentTime.Sub(rf.lastHeartbeatTime).Milliseconds() > int64(rf.leaderElectionTimeout) {
				log.Printf("%v at Term %v is election timeout! try request vote!", rf.me, rf.currentTerm)
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
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	// heartBeat interval not exceed than 150ms, election timeout not exceed 500 but larger one heartbeat interval
	rf.heartBeatInterval = 100 + rand.Intn(50)
	// rf.leaderElectionTimeout = 200 + rand.Intn(300)
	rf.resetElectionTimeout(200, 300)
	rf.isLeader = false
	// init leader's state
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.state = follower
	rf.lastHeartbeatTime = time.Now()
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
	// log.SetFlags(0)
	// log.SetOutput(ioutil.Discard)
	// add point about raft start, report state point
	log.Printf("raft %v instance make finish, current term %v, logEntires %v", rf.me, rf.currentTerm, rf.logEntries)
	// start election time out check
	go rf.electionTimeoutCheck()

	return rf
}
