package raft

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		// 阻塞等待其他peer把日志应用，
		rf.applyCond.Wait()
		// 构造日志
		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log[i])
		}
		rf.mu.Unlock()

		// 发送给上层调用者，往applyCh中输送，这里因为是往channel中写，不知道上层应用什么时候读取channel中的数据
		// 所以不能长期持有锁，阻塞，会让其他协程获取不到锁。
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandVaild,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1 + i, // i从0开始
			}
		}

		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d,%d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}
}
