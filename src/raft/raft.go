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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Second
	electionTimeoutMax time.Duration = 400 * time.Second
	replInterval       time.Duration = 200 * time.Millisecond
)

const (
	InvalidTerm  = 0
	InvalidIndex = 0
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role        Role
	currentTerm int
	votedFor    int // -1 means hasn't voted yet , 票投给了哪个候选者

	// 应用日志
	commitIndex int
	lastApplied int
	applyCond   *sync.Cond
	applyCh     chan ApplyMsg

	log []LogEntry // 每一个peer自己本地的log记录

	nextIndex  []int // Leader拥有的记录每个不同peer的应该同步的下一条日志数组NextIndex
	matchIndex []int // Lader拥有的记录每个不同peer已经匹配好的日志数组matchIndex

	electionStart   time.Time
	electionTimeout time.Duration // randomized election timeout
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become follower, lower term: T%d", term)
		return
	}
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%d->T%d", rf.role, rf.currentTerm, term)
	rf.role = Follower
	shouldPersist := term != rf.currentTerm
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	// 持久化，任期发生了改变
	if shouldPersist {
		rf.persistLocked()
	}
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become candidate")
		return
	}
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	// 持久化，votedFor和任期均发生了改变
	rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only candidate can become leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
	// 初始化自己的各跟随者的matchIndex和nextIndex数组
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// Start the service using Raft (e.g. a k/v server) wants to start
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
// Start Leader接受应用层命令函数，要将 `command` 包裹为日志，然后追加到本地日志中，也需要持久化
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log = append(rf.log, LogEntry{
		CommandVaild: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	rf.persistLocked()

	return len(rf.log) - 1, rf.currentTerm, true
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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

func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.role == role && rf.currentTerm == term)
}

func (rf *Raft) stateString() string {
	return fmt.Sprintf("Term[%d],votedFor:%d,log:[0,%d)", rf.currentTerm, rf.votedFor, len(rf.log)-1)
}

// 找到不同任期内的第一条日志
func (rf *Raft) firstIndex(term int) int {
	for idx, entry := range rf.log {
		if entry.Term == term {
			return idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

// Make the service or tester wants to create a Raft server. the ports
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

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// 初始化apply日志
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.log = append(rf.log, LogEntry{Term: InvalidTerm})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applyTicker()

	return rf
}
