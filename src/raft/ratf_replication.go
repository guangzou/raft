package raft

import (
	"sort"
	"time"
)

// AppendEntries Follwer对Leader的心跳响应
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// align the term
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject, higher term, T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if rf.currentTerm <= args.Term {
		rf.becomeFollowerLocked(args.Term)
	}

	// 如果 Leader 和 Follower 匹配日志所花时间特别长，Follower 一直不重置选举时钟，就有可能错误的选举超时触发选举
	// 无论 Follower 接受还是拒绝日志，只要认可对方是 Leader 就要重置时钟
	defer rf.resetElectionTimerLocked()

	prevIndex, prevTerm := args.PrevLogIndex, args.PrevLogTerm
	// 要同步的日志索引比我本地的日志长度还要大，说明此时我已经很久没有同步日志了
	if prevIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Reject log, it's log too short, len:%d < prev:%d",
			args.LeaderId, len(rf.log), prevIndex)
		reply.ConfilictIndex = len(rf.log)
		reply.ConfilictTerm = InvalidTerm
		return
	}

	// 日志任期不一致
	if rf.log[prevIndex].Term != prevTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Reject log, Prev log not match, [%d]: T%d != T%d",
			args.LeaderId, prevIndex, prevTerm, rf.log[prevIndex].Term)
		reply.ConfilictTerm = rf.log[prevIndex].Term
		reply.ConfilictIndex = rf.firstIndex(reply.ConfilictTerm)
		return
	}
	// 更新本地日志，截断日志，从prevIndex处后面添加新的日志
	rf.log = append(rf.log[:prevIndex+1], args.Entries...)
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept log: (%d, %d]", prevIndex, prevIndex+len(args.Entries))

	// 更新commitIndex
	if args.LeaderCommitIndex > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update commitIndex %d -> %d", rf.commitIndex, args.LeaderCommitIndex)
		rf.commitIndex = args.LeaderCommitIndex
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(p int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[p].Call("Raft.AppendEntries", args, reply)
	return ok
}

// replicationTicker 定时发送心跳
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replInterval)
	}
}

type LogEntry struct {
	Command      interface{} // 执行的指令
	CommandVaild bool        // 是否是操作数据的执行指令
	Term         int
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int // 上一条日志在日志数组里的索引位置
	PrevLogTerm       int // 上一条日志的任期
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Follower 和 Leader 冲突日志的信息
	ConfilictIndex int
	ConfilictTerm  int
}

func (rf *Raft) startReplication(term int) bool {

	replicateToPeer := func(p int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(p, args, reply)
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Lost or error", p)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		if !reply.Success {
			prevNext := rf.nextIndex[p]
			// 该follower的日志太短
			if reply.ConfilictTerm == InvalidTerm {
				rf.nextIndex[p] = reply.ConfilictIndex
			} else {
				// 以 Leader 日志为准，跳过 ConfilictTerm 的所有日志
				// 如果发现 Leader 日志中不存在 ConfilictTerm的任何日志，
				// 则以 Follower 为准跳过 ConflictTerm，即使用 ConfilictIndex。
				firstTermIndex := rf.firstIndex(reply.ConfilictTerm)
				if firstTermIndex != InvalidIndex {
					rf.nextIndex[p] = firstTermIndex + 1
				} else {
					rf.nextIndex[p] = reply.ConfilictIndex
				}
			}
			// 匹配探测期比较长时，会有多个探测的 RPC，如果 RPC 结果乱序回来：一个先发出去的探测 RPC 后回来了，
			//其中所携带的 ConfilictTerm和 ConfilictIndex就有可能造成 rf.next 的“反复横跳”。
			//为此，制 rf.next`单调递减
			if rf.nextIndex[p] > prevNext {
				rf.nextIndex[p] = prevNext
			}
			return
		}
		// 更新Leader的matchIndex和nextIndex
		rf.matchIndex[p] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[p] = rf.matchIndex[p] + 1

		majorIndex := rf.getMajorPeerMatchIndexLocked()
		if majorIndex > rf.commitIndex {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update commitIndex %d -> %d", rf.commitIndex, majorIndex)
			rf.commitIndex = majorIndex
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Lost context, abort replication in Leader[T%d] -> T%d", rf.currentTerm, term)
		return false
	}

	for p := range rf.peers {
		if p == rf.me {
			// 更新leader自己的数组
			rf.matchIndex[p] = len(rf.log) - 1
			rf.nextIndex[p] = len(rf.log)
			continue
		}
		prevIndex := rf.nextIndex[p] - 1
		prevTerm := rf.log[prevIndex].Term
		args := &AppendEntriesArgs{
			Term:              term,
			LeaderId:          rf.me,
			PrevLogIndex:      prevIndex,
			PrevLogTerm:       prevTerm,
			Entries:           rf.log[prevIndex+1:],
			LeaderCommitIndex: rf.commitIndex,
		}
		go replicateToPeer(p, args)
	}
	return true
}

// 获取大多数peer中matchIndex的位置，排好序，取多数派中的那个matchIndex
func (rf *Raft) getMajorPeerMatchIndexLocked() int {
	tmpSlice := make([]int, len(rf.peers))
	copy(tmpSlice, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpSlice))
	majorIndex := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "MatchIndex after sort: %v,majorIndex=%d which in matchIndex[%d]", rf.matchIndex, majorIndex, tmpSlice[majorIndex])
	return tmpSlice[majorIndex]
}
