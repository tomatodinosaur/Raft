rf.mu.Lock()
rf.currentTerm++
rf.state="candidate"
vote:=1
finished:=1
term:=rf.currentTerm
id:=rf.me
lastlogindex:=0
lastlogterm:=0
rf.votedFor=rf.me
rf.mu.Unlock()
cond:=sync.NewCond(&rf.mu)
DPrintf("[%d] 开始申票 state:{%v} term:{%d}\n",rf.me,rf.state,rf.currentTerm)
for i:=0;i<len(rf.peers);i++ {
	if i==rf.me{
		continue
	}
	go func(p int){
			Args:=RequestVoteArgs{term,id,lastlogindex,lastlogterm}
			Reply:=RequestVoteReply{}
			ok:=rf.sendRequestVote(p,&Args,&Reply);
			
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer cond.Broadcast()

			finished++

			if !ok{
				return
			}
			
			//防止异常
			if term!=rf.currentTerm || rf.state=="follower"{
				return
			}
			//有效状态为Reply.term == term
			//无效状态为Reply.term >term
			
			//自己的term已经过期了
			if Reply.Term>term {
				rf.Convertfollower(Reply.Term)
				rf.updatelasttime()
				return
			}
			//判断得票
			if Reply.VoteGranted {
				vote++;
			}
		}(i)
	}

	rf.mu.Lock()
	for vote<=len(rf.peers)/2 && finished<len(rf.peers){
		cond.Wait()
	}
	//选举超时 或 已经有leader；
	if term!=rf.currentTerm ||rf.state=="follower"{
		DPrintf("[%d] 意外情况退出竞选 state:{%v} term:{%d}\n",rf.me,rf.state,rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	//选中
	if(vote>len(rf.peers)/2 && rf.state=="candidate"){
		rf.state="leader"
		for i:=0;i<len(rf.peers);i++ {
			if i==rf.me{
				continue
			}
			rf.nextindex[i]=rf.lastlog().Index+1
			rf.matchindex[i]=0
		}
		
		DPrintf("[%d] 得票%d，当选leader state:{%v} term:{%d}\n",rf.me,vote,rf.state,rf.currentTerm)
		rf.mu.Unlock()
		//并行发送心跳：
	//	DPrintf("%d  %d  %d\n",vote,len(rf.peers),finished)
		go rf.heart(term)

	}else{
		DPrintf("[%d] 得票%d,竞选失败退出竞选 state:{%v} term:{%d}\n",rf.me,vote,rf.state,rf.currentTerm)
		rf.Convertfollower(rf.currentTerm)
		rf.mu.Unlock()
	}


}