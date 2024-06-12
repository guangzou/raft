package raft

import "time"

// AppendEntries 心跳响应
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// align the term
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DLog, "<- S%d, Reject, higher term, T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if rf.currentTerm <= args.Term {
		rf.becomeFollowerLocked(args.Term)
	}
	rf.resetElectionTimerLocked()
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Lost context, abort replication in Leader[T%d] -> T%d", rf.currentTerm, term)
		return false
	}

	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     term,
			LeaderId: rf.me,
		}
		go replicateToPeer(p, args)
	}
	return true
}
