package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) isNewLocked(candidateIndex, candidateTerm int) bool {
	l := len(rf.log)
	lastIndex, lastTerm := l-1, rf.log[l-1].Term
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	// 任期不相等，Term高者更新
	if candidateTerm != lastTerm {
		return lastTerm > candidateTerm
	}
	// 任期相等，index大者更新
	return lastIndex > candidateIndex
}

// resetElectionTimerLocked 重置选举定时器
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// isElectionTimeoutLocked 判断选举计时器是否超时
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool // 是否投票
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// align the term
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if rf.currentTerm < args.Term {
		rf.becomeFollowerLocked(args.Term)
	}

	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Already voted S%d", args.CandidateId, rf.votedFor)
		return
	}

	if rf.isNewLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, S%d's log less up-to-date", args.CandidateId)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d", args.CandidateId)
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

func (rf *Raft) startElection(term int) bool {
	votes := 0
	askVoteFromPeer := func(p int, args *RequestVoteArgs) {
		// 发送rpc请求给 peer and 处理相应
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(p, args, reply)

		// 处理响应
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from %d, Lost or error", p)
			return
		}

		// 对其任期 align the term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(Candidate, rf.currentTerm) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply in T%d", rf.currentTerm)
			return
		}

		// 计票
		if reply.VoteGranted {
			votes++
		}
		if votes >= len(rf.peers)/2 {
			rf.becomeLeaderLocked()
			// 成为leader后，开始向其他peer发送心跳和数据
			go rf.replicationTicker(term)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 判断是否已经丢失了上下文，指Term和role。即在一个任期内，只要角色没有变化，就能放心推进状态机
	if rf.contextLostLocked(Candidate, term) {
		return false
	}
	l := len(rf.log)
	for p := range rf.peers {
		if p == rf.me {
			votes++
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: l - 1,
			LastLogTerm:  rf.log[l-1].Term,
		}
		go askVoteFromPeer(p, args)
	}
	return true
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
