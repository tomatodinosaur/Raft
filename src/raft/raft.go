package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
// create a new Raft server instance:
// rf := Make(peers, me, persister, applyCh)

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
	"sync"
	"sync/atomic"
	"time"
	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
 )

func min(a int,b int) int{
	if a<b{
		return a
	}
	return b
 }
func max(a int,b int) int{
	if a>b{
		return a
	}
	return b
 }

const Heartinterval = 50
const Electioninterval = 150

type Op struct {

	Type string 
	Key string
	Value string
	Clientid int64
	Commandid int
 }
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term 				 int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
 }

type Entry struct {
	Term int
	Index int
	Command interface{}
 }
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	state string
	lastreceive int64

	applyChan chan ApplyMsg
	log []Entry //从1开始
	nextindex []int 
	matchindex []int
	commitindex int //初始化为0
	lastapplied int //初始化为0
	snapshotData []byte
	applyCond      *sync.Cond 
 }

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock();
	defer rf.mu.Unlock();
	term=rf.currentTerm;
	isleader=(rf.state=="leader")
	return term, isleader
 }

func (rf *Raft) lastlog() (Entry) {
	l:=len(rf.log)
	return rf.log[l-1];
 }

func (rf *Raft) Show() {
	DPrintf("[%d] commit: %d applied: %d firstindex: %d lastindex: %d term: %d\n",rf.me,rf.commitindex,rf.lastapplied,rf.log[0].Index,rf.lastlog().Index,rf.lastlog().Term)
 }

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
 }

func (rf *Raft) persistStateAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
  e.Encode(rf.votedFor)
  e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshotData)
 }

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var log []Entry
	if d.Decode(&term) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil  {
		DPrintf("Error: raft%d readPersist.", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.log = log
		DPrintf("[%d] state:{%v} term:{%d} 提取term:%d,voteFor:%d,log日志长度:%d\n",rf.me,rf.state,rf.currentTerm,rf.currentTerm,rf.votedFor,len(rf.log)-1)
		rf.mu.Unlock()
		}
 }

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitindex {
		return false
	}

	if lastIncludedIndex>rf.lastlog().Index{
		rf.log=rf.log[:1]
	}else {
		//[lastIncludeIndex:]
		for Index, entry := range rf.log {
			if entry.Index == lastIncludedIndex {
					rf.log = rf.log[Index:]
			}	
		}
	}
	rf.snapshotData=snapshot
	rf.log[0].Index=lastIncludedIndex
	rf.log[0].Term=lastIncludedTerm
	rf.commitindex=lastIncludedIndex
	rf.lastapplied=lastIncludedIndex
	DPrintf("[%d] 快照安装成功,commit: %d applied: %d firstindex: %d lastindex: %d term: %d\n",rf.me,rf.commitindex,rf.lastapplied,rf.log[0].Index,rf.lastlog().Index,rf.lastlog().Term)
	rf.persistStateAndSnapshot()
	return true
 }

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// index是快照最后包含的index
	// 0     1        2     ... index ...   lastlogindex
	// index index+1  lastlogindex
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index>rf.lastlog().Index || index<rf.log[0].Index || index>rf.commitindex {
		return
	}
	for Index, entry := range rf.log {
		if entry.Index == index {
				rf.log = rf.log[Index:]
		}	
	}
	// rf.log=rf.log[index-rf.log[0].Index:]
	DPrintf("[%d] state:{%v} term:{%d} 快照生成Lastincludeindex:%d \n",rf.me,rf.state,rf.currentTerm,rf.log[0].Index)
	rf.Show()
	rf.snapshotData=snapshot
	rf.persistStateAndSnapshot()
 }




type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int //sender id
	Candidateid int//sender id
	
	Lastlogindex int//自己最后一个日志号
	Lastlogterm int//自己最后一个日志任期号
 }

type AppendEntriesArgs struct {
	Term int
	Leaderid int
	//
	Prelogindex int //前一个日志的日志号\紧接新条目之前的条目的索引
	Prelogterm int //前一个日志的日期号\紧接新条目之前的条目的任期
	Entries []Entry  //当前日志体
	Leadercommit int //leader已提交日志号
 }

type InstallSnapshotArgs struct {
	Term int
	Leaderid int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
 }




type RequestVoteReply struct {
	// Your data here (2A).
	Term int //receiver term
	VoteGranted bool //if vote

 }

type AppendEntriesReply struct {
	Term int
	Success bool //如果follower包括前一个日志，则返回true
	Xterm int
	Xindex int
	Xlen int
 }

type InstallSnapshotReply struct{
	Term int
 }


//handle installsnapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
			return
	}
	if rf.currentTerm< args.Term {
		rf.Convertfollower(args.Term)
	}
	rf.updatelasttime()
	rf.persist()
	if args.LastIncludedIndex<=rf.commitindex {
		return
	}
	applymsg:=ApplyMsg {
		SnapshotValid :true,
		Snapshot:args.Data,
		SnapshotTerm :args.LastIncludedTerm,
		SnapshotIndex:args.LastIncludedIndex,
	}
	go func(msg ApplyMsg)	{
		rf.applyChan<-msg
	}(applymsg)
 }
//handle requestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// rpc失效,周期失效,告知sender当前最新的term
	if rf.currentTerm > args.Term {
	//	DPrintf("[%d] state:{%v} term:{%d} 周期过期 拒绝投票 [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,args.Candidateid,args.Term)
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
		return
	}
	// rpc的term更新,更新自己的term,并告知sender已经更新，回复有效

	// leader收到更新的投票请求,不能重置选举时间
	if rf.currentTerm< args.Term {
		rf.Convertfollower(args.Term)
	}

	reply.Term=rf.currentTerm
	k:=true
	if args.Lastlogterm<rf.lastlog().Term {
		k=false
	}
	if args.Lastlogterm==rf.lastlog().Term {
		k=(args.Lastlogindex >= rf.lastlog().Index)
	}
	DPrintf("%d vote to %d,args.term:%d args.index:%d, lastterm:%d lastindex %d",rf.me,args.Candidateid,args.Lastlogterm,args.Lastlogindex,rf.lastlog().Term,rf.lastlog().Index)
	//if votedfor is null or candidateid
	if (rf.votedFor==-1 || rf.votedFor==args.Candidateid)&& k {
		reply.VoteGranted=true
		rf.votedFor=args.Candidateid
		rf.updatelasttime()
		DPrintf("[%d] state:{%v} term:{%d} 同意投票 [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,args.Candidateid,args.Term)
	}else {
		reply.VoteGranted=false
		DPrintf("[%d] state:{%v} term:{%d} 拒绝投票,k==%v [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,k,args.Candidateid,args.Term)
	}
 }

//handle appendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// rpc失效,周期失效,告知sender当前最新的term
	if rf.currentTerm > args.Term {
		reply.Success=false
		reply.Term=rf.currentTerm
	//DPrintf("[%d] state:{%v} term:{%d} 周期过期 拒绝心跳 [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,args.Leaderid,args.Term)
		return
	}

	if rf.currentTerm< args.Term {
		rf.Convertfollower(args.Term)
	}
	
	// rpc的term更新,更新自己的term,并告知sender已经更新，回复有效
	if rf.state=="candidate" {
		rf.Convertfollower(args.Term)
	}
	reply.Term=rf.currentTerm
	rf.updatelasttime();

	//都是follower
	//1：if 自己的log更短
	if rf.lastlog().Index<args.Prelogindex{
		DPrintf("[%d] state:{%v} term:{%d} log更短,拒绝append [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,args.Leaderid,args.Term)
		reply.Xterm=-1
		reply.Xindex=-1
		reply.Xlen=rf.lastlog().Index+1
		reply.Success=false
		return
	}
	//DPrintf("%d %d\n",args.Prelogindex,rf.log[0].Index)
	//2D if Prelogindex在快照里，让 leader 下次从快照外面发
	if args.Prelogindex < rf.log[0].Index {
		reply.Xterm=-1
		reply.Xindex=-1
		reply.Xlen=rf.log[0].Index+1
		reply.Success=false
		return
	}

	//2:: if Prelogindex处有log，但周期不同
	preterm:=rf.log[args.Prelogindex-rf.log[0].Index].Term
	if preterm!=args.Prelogterm {
		DPrintf("[%d] state:{%v} term:{%d} log所在周期【%d != %d】不同 [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,preterm,args.Prelogterm,args.Leaderid,args.Term)
		reply.Xterm=preterm
		for j:=args.Prelogindex;j>=rf.log[0].Index;j--{
			if rf.log[j-rf.log[0].Index].Term!=preterm {
				reply.Xindex=j+1
				break;
			}
		}
		reply.Success=false
		return
	}

	//正常配对下，只截取周期不一致的多出来的部分，不然会被错序的心跳嘎掉
	for idx, entry := range args.Entries {
		//2D增加跳过快照中的追加信息
		if entry.Index <= rf.log[0].Index {
			continue
		}
		if entry.Index <= rf.lastlog().Index && rf.log[entry.Index-rf.log[0].Index].Term != entry.Term {
				rf.log=rf.log[0:entry.Index-rf.log[0].Index]
		}
		// Follower 复制 log
		if entry.Index > rf.lastlog().Index {
				rf.log=append(rf.log,args.Entries[idx:]...)
				break
		}
 }
	// //正常配对:即preindex.term=preterm:		
	// //Follower截取自己在preindex之后的日志
	// rf.log=rf.log[0:args.Prelogindex+1]
	// //从preindex+1一直复制完 Entries
	// for j:=0;j<len(args.Entries);j++ {
	// 	rf.log=append(rf.log,args.Entries[j])
	// }
	//更新当前节点提交进度
	if args.Leadercommit>rf.commitindex {
		rf.commitindex=min(args.Leadercommit,rf.lastlog().Index)
		DPrintf("[%d] state:{%v} term:{%d} 提交更新到%d\n",rf.me,rf.state,rf.currentTerm,rf.commitindex)
		rf.applyCond.Broadcast()
	}
	rf.Show()
	reply.Success=true
	DPrintf("[%d] state:{%v} term:{%d} 接受append,当前Lastindex=%d [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,rf.lastlog().Index,args.Leaderid,args.Term)
	return

 }



func (rf *Raft) Convertfollower (term int) {
	rf.state="follower"
	rf.currentTerm=term
	rf.votedFor=-1
 }

func (rf *Raft) updatelasttime(){
	rf.lastreceive=time.Now().UnixNano() / int64(time.Millisecond)
 }

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
 }
func (rf *Raft) sendappendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
 }
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
 }

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed()==true {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state!="leader"{
		return index,rf.currentTerm,false
	}

	newEntry:=Entry{
		Term: rf.currentTerm,
		Index:rf.lastlog().Index+1,
		Command: command,
	}
	//index=len(rf.log)
	index=rf.lastlog().Index+1
	rf.log=append(rf.log,newEntry)
	term=rf.currentTerm
	DPrintf("[%d] 接到命令%d state:{%v} term:{%d}\n",rf.me,index,rf.state,rf.currentTerm)
	go rf.attempappend(term)
	return index, term, isLeader
 }


func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
 }

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
 }

func (rf *Raft) ticker() {
		for rf.killed() == false{
			Electiontimeout:=Electioninterval +rand.Intn(150)
			rf.mu.Lock()
			//心跳超时
			start_time:=time.Now().UnixNano() / int64(time.Millisecond)
			delt_time:=int(start_time-rf.lastreceive)
			if( (delt_time>Electiontimeout) &&  (rf.state!="leader") ){//or follower
				DPrintf("[%d] 心跳超时，开始竞选 state:{%v} term:{%d}\n",rf.me,rf.state,rf.currentTerm)
				go rf.attempeletion()
			}
			rf.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
		}
 }
func (rf *Raft) attempeletion() {
		rf.mu.Lock()
		rf.currentTerm++
		rf.state="candidate"
		vote:=1
		rf.updatelasttime()
		finished:=1
		term:=rf.currentTerm
		id:=rf.me
		lastlogindex:=rf.lastlog().Index
		lastlogterm:=rf.lastlog().Term
		rf.votedFor=rf.me
		rf.persist()
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
					
					if !ok{
						return
					}
					
					rf.mu.Lock()
					defer rf.mu.Unlock()
					defer cond.Broadcast()

					finished++

					//防止异常
					if term!=rf.currentTerm || rf.state=="follower"{
						DPrintf("[%d] 在%d轮选举死于防止异常 state:{%v} term:{%d}\n",rf.me,term,rf.state,rf.currentTerm)
						return
					}
					//有效状态为Reply.term == term
					//无效状态为Reply.term >term
					
					//自己的term已经过期了
					if Reply.Term>term {
						DPrintf("[%d] 在%d轮选举死于周期过期 state:{%v} term:{%d}\n",rf.me,term,rf.state,rf.currentTerm)
						rf.Convertfollower(Reply.Term)
						rf.persist()
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
			DPrintf("[%d] 在%d轮选举已经到达这里 state:{%v} term:{%d}\n",rf.me,term,rf.state,rf.currentTerm)
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
				rf.persist()
				rf.mu.Unlock()
			}


		}

func (rf *Raft) heart(term int) {
	Hearttimeout:=Heartinterval
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state!="leader"{
			DPrintf("[%d] 停止发送心跳 state:{%v} term:{%d}\n",rf.me,rf.state,rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.attempappend(term)
		time.Sleep(time.Duration(Hearttimeout) * time.Millisecond)
	}
 }

func (rf *Raft) attempappend(term int)  {
	finished:=1
	vote:=1
	//DPrintf("[%d] 开始append,当前Lastindex=%d state:{%v} term:{%d}\n",rf.me,rf.lastlog().Index,rf.state,rf.currentTerm)
	for i:=0;i<len(rf.peers);i++ {
		if i==rf.me {
			continue
		}
		//并行发送appendRPC
		go func(p int){
			rf.mu.Lock()
			args:=AppendEntriesArgs {
				Term:term,
				Leaderid:rf.me,
				Leadercommit:rf.commitindex}
			nextindex:=rf.nextindex[p]
			//2D:
				DPrintf("[%d] nextindex:[0] %d [1] %d [2] %d\n",rf.me,rf.nextindex[0],rf.nextindex[1],rf.nextindex[2])
			if nextindex<=rf.log[0].Index {
				DPrintf("[%d] 发送快照,nextindex=%d state:{%v} term:{%d}\n",rf.me,nextindex,rf.state,rf.currentTerm)
				snapshotargs:=InstallSnapshotArgs{
					Term:rf.currentTerm,
					Leaderid:rf.me,
					LastIncludedIndex:rf.log[0].Index,
					LastIncludedTerm:rf.log[0].Term,
					Data:rf.snapshotData,
				}
				snapshotreply:=InstallSnapshotReply{}
				rf.mu.Unlock()
				ok:=rf.sendInstallSnapshot(p,&snapshotargs,&snapshotreply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer rf.persist()
				finished++

				if !ok{
					return
				}
	
				if term!=rf.currentTerm || rf.state!="leader"{
					return
				}
	
				if snapshotreply.Term>rf.currentTerm {
					rf.Convertfollower(snapshotreply.Term)
					return
				}

				rf.matchindex[p]=snapshotargs.LastIncludedIndex
				rf.nextindex[p]=rf.matchindex[p]+1
				return 
			}

			//
			args.Prelogindex=nextindex-1;
			args.Prelogterm=rf.log[args.Prelogindex-rf.log[0].Index].Term
			for j:=nextindex;j<=rf.lastlog().Index;j++ {
				args.Entries=append(args.Entries,rf.log[j-rf.log[0].Index])
			}

			reply:=AppendEntriesReply{}
			rf.mu.Unlock()
			
			ok:=rf.sendappendEntries(p,&args,&reply)
			//DPrintf("[%d] 已发送给%d state:{%v} term:{%d}\n",rf.me,p,rf.state,rf.currentTerm)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()

			finished++

			if !ok{
				return
			}

			if term!=rf.currentTerm || rf.state!="leader"{
				return
			}

			if reply.Term>rf.currentTerm {
				rf.Convertfollower(reply.Term)
				return
			}
 //append成功，意味着直接更新到了旧lastlogindex：Prelogindex+len(Entries)
			if reply.Success==true {
				vote++
				rf.matchindex[p]=args.Prelogindex+len(args.Entries)
				rf.nextindex[p]=rf.matchindex[p]+1
			}
 //append失败，快速回滚
			if reply.Success==false {
				//1:x.term==-1 :			
				//Follower 日志少，下一次从 XLen 开始
				if reply.Xterm==-1 {
					rf.nextindex[p]=reply.Xlen
				}
				if reply.Xterm!=-1{
					Next:=-1
					for j:=args.Prelogindex;j>=rf.log[0].Index;j--{
						if rf.log[j-rf.log[0].Index].Term==reply.Xterm{
							Next=j+1
							break;
						}
					}	
					//2:Leader中存在x.term:
					if Next!=-1{
						rf.nextindex[p]=Next
					}
					//3:Leader中不存在x.term：
					if Next==-1{
						rf.nextindex[p]=reply.Xindex
					}
				}
			}			
		}(i)
	}
	//Leader 开始提交任务
	rf.Leadercommit()
 }

func (rf *Raft) Leadercommit(){
	//只能提交自己周期的任务，其他周期的任务顺带提交
	rf.mu.Lock()
  defer rf.mu.Unlock()
	for i:=rf.commitindex+1;i<=rf.lastlog().Index;i++ {
		if rf.log[i-rf.log[0].Index].Term!=rf.currentTerm {
			continue
		}
		count:=1;
		for j:=0;j<len(rf.peers);j++ {

			if j==rf.me {
				continue
			}

			if rf.matchindex[j]>=i{
				count++;
			}
		}
		if count>len(rf.peers)/2 {
			rf.commitindex=i
			DPrintf("[%d] state:{%v} term:{%d} 响应过半,提交到%d\n",rf.me,rf.state,rf.currentTerm,rf.commitindex)
		}
	}
	rf.applyCond.Broadcast()
 }

func (rf *Raft) apply(){

		rf.applyCond=sync.NewCond(&rf.mu)
		for !rf.killed() {
      rf.mu.Lock()
      for rf.lastapplied >= rf.commitindex {
         rf.applyCond.Wait()
      }
			if rf.log[0].Index > rf.lastapplied+1 {
				rf.mu.Unlock()
				return 
			}
      commitIndex := rf.commitindex
      lastApplied := rf.lastapplied
      applyEntries:=rf.log[lastApplied+1-rf.log[0].Index:commitIndex+1-rf.log[0].Index]
			rf.mu.Unlock()
      //解锁后进行apply
      for _, entry := range applyEntries {
         rf.applyChan <- ApplyMsg{
            CommandValid: true,
            Command:      entry.Command,
            CommandIndex: entry.Index,
						Term        : entry.Term,
         }
      }
      rf.mu.Lock()
      rf.lastapplied = max(rf.lastapplied, commitIndex)
			DPrintf("[%d] state:{%v} term:{%d} 应用到%d\n",rf.me,rf.state,rf.currentTerm,rf.lastapplied)
      rf.mu.Unlock()
   }
  }


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.lastreceive=time.Now().UnixNano() / int64(time.Millisecond)
	rf.state="follower"
	rf.votedFor=-1
	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh
	rf.commitindex=0
	rf.lastapplied=0
	rf.log=make([]Entry,0);
	rf.log=append(rf.log,Entry{-1,0,0})
	
	rf.matchindex=make([]int ,len(rf.peers))
	rf.nextindex=make([]int ,len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshotData = persister.snapshot
	rf.lastapplied = rf.log[0].Index
	rf.commitindex = rf.log[0].Index
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()

	return rf
 }
 func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}