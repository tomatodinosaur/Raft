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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
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

const Heartinterval = 200
const Electioninterval = 500

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
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
}

// return currentTerm and whether this server believes it is the leader.
// ask a Raft for its current term, and whether it thinks it is leader
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
	DPrintf("[%d] commit: %d applied: %d index: %d term: %d\n",rf.me,rf.commitindex,rf.lastapplied,rf.lastlog().Index,rf.lastlog().Term)
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 将Raft的持久化状态保存到稳定存储中，
// 以便在发生崩溃并重新启动后可以稍后检索。
// 有关应该持久化的内容的描述，请参见论文中的图2。

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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//


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
	//if Leadercommit > commitindex:
	//	commitindex=min(Leadercommit,index of last new entry)
}
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//


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
//
// example RequestVote RPC handler.
//

//handle requestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rpc失效,周期失效,告知sender当前最新的term
	if rf.currentTerm > args.Term {
		DPrintf("[%d] state:{%v} term:{%d} 周期过期 拒绝投票 [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,args.Candidateid,args.Term)
		reply.VoteGranted=false
		reply.Term=rf.currentTerm
		return
	}
	// rpc的term更新,更新自己的term,并告知sender已经更新，回复有效

	// leader收到更新的投票请求
	if rf.currentTerm< args.Term {
		rf.Convertfollower(args.Term)
		rf.updatelasttime();
		rf.votedFor=args.Candidateid
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
		DPrintf("[%d] state:{%v} term:{%d} 拒绝投票,k==%v [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,k,args.Candidateid,args.Term)
		
	}
 }

//handle appendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rpc失效,周期失效,告知sender当前最新的term
	if rf.currentTerm > args.Term {
		reply.Success=false
		reply.Term=rf.currentTerm
		DPrintf("[%d] state:{%v} term:{%d} 周期过期 拒绝心跳 [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,args.Leaderid,args.Term)
		return
	}
	// rpc的term更新,更新自己的term,并告知sender已经更新，回复有效
	reply.Success=true
	reply.Term=rf.currentTerm
	rf.Convertfollower(args.Term)
	rf.updatelasttime();
	if len(args.Entries)==0 {
		//心跳
		DPrintf("[%d] state:{%v} term:{%d} 重置时间，接受心跳 [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,args.Leaderid,args.Term)
		//handle心跳的过程中提交自己的任务
		if args.Leadercommit>rf.commitindex {
			rf.commitindex=min(args.Leadercommit,rf.lastlog().Index)
			rf.apply()
		}
		rf.Show()
		return
	}
	
	//都是follower
	
	//1：if 自己的log更短
	if rf.lastlog().Index<args.Prelogindex{
		DPrintf("%d  %d",rf.lastlog().Index,args.Prelogindex)
		DPrintf("[%d] state:{%v} term:{%d} log更短,拒绝append [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,args.Leaderid,args.Term)
		reply.Xterm=-1
		reply.Xindex=-1
		reply.Xlen=rf.lastlog().Index+1
		reply.Success=false
		return
	}
	//2:: if Prelogindex处有log，但周期不同
	preterm:=rf.log[args.Prelogindex].Term
	if preterm!=args.Prelogterm {
		DPrintf("[%d] state:{%v} term:{%d} log所在周期【%d != %d】不同 [%d] term:{%d}\n",rf.me,rf.state,rf.currentTerm,preterm,args.Prelogterm,args.Leaderid,args.Term)
		reply.Xterm=preterm
		for j:=args.Prelogindex;j>=0;j--{
			if rf.log[j].Term!=preterm {
				reply.Xindex=j+1
				break;
			}
		}
		reply.Success=false
		return
	}

	//正常配对:即preindex.term=preterm:		
	//Follower截取自己在preindex之后的日志
	rf.log=rf.log[0:args.Prelogindex+1]
	//从preindex+1一直复制完 Entries
	for j:=0;j<len(args.Entries);j++ {
		rf.log=append(rf.log,args.Entries[j])
	}
	//更新当前节点提交进度
	if args.Leadercommit>rf.commitindex {
		rf.commitindex=min(args.Leadercommit,rf.lastlog().Index)
		DPrintf("[%d] state:{%v} term:{%d} 提交更新到%d\n",rf.me,rf.state,rf.currentTerm,rf.commitindex)
		rf.apply()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
 }
func (rf *Raft) sendappendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	if rf.killed()==true {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state!="leader"{
		return index,rf.currentTerm,false
	}

	newEntry:=Entry{
		Term: rf.currentTerm,
		Index:rf.lastlog().Index+1,
		Command: command,
	}
	index=len(rf.log)
	rf.log=append(rf.log,newEntry)
	term=rf.currentTerm
	DPrintf("[%d] 接到命令 state:{%v} term:{%d}\n",rf.me,rf.state,rf.currentTerm)
	go rf.attempappend(term)
	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
		for rf.killed() == false{
			Electiontimeout:=Electioninterval +rand.Intn(250)
			rf.mu.Lock()
			//心跳超时
			start_time:=time.Now().UnixNano() / int64(time.Millisecond)
			delt_time:=int(start_time-rf.lastreceive)
			if( (delt_time>Electiontimeout) &&  (rf.state!="leader") ){//or follower
				DPrintf("[%d] 心跳超时，开始竞选 state:{%v} term:{%d}\n",rf.me,rf.state,rf.currentTerm)
				rf.updatelasttime()
				go rf.attempeletion()
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	
 }
func (rf *Raft) attempeletion() {
		rf.mu.Lock()
		rf.currentTerm++
		rf.state="candidate"
		vote:=1
		finished:=1
		term:=rf.currentTerm
		id:=rf.me
		lastlogindex:=rf.lastlog().Index
		lastlogterm:=rf.lastlog().Term
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
		

	//	cond:=sync.NewCond(&rf.mu)
		DPrintf("[%d] 发送心跳 state:{%v} term:{%d}\n",rf.me,rf.state,rf.currentTerm)
		rf.Show()
		for i:=0;i<len(rf.peers);i++{
			if i==rf.me {
				rf.mu.Lock()
				rf.updatelasttime()
				rf.mu.Unlock()
				continue
			}
			go func(p int){
				Args:=AppendEntriesArgs{}
				Reply:=AppendEntriesReply{}
				Args.Term=term
				Args.Leaderid=rf.me
				Args.Leadercommit=rf.commitindex
				//发出心跳
				ok:=rf.sendappendEntries(p,&Args,&Reply);

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !ok{
					return
				}
				//防止异常
				if term!=rf.currentTerm{
					return
				}

			//判断Reply是否有效,待定
				if Reply.Term>term {
					rf.Convertfollower(Reply.Term)
					rf.updatelasttime()
					return
				}
			}(i)
		}
		rf.Leadercommit()
		time.Sleep(time.Duration(Hearttimeout) * time.Millisecond)
	}
 }

func (rf *Raft) attempappend(term int)  {
	finished:=1
	vote:=1
	cond:=sync.NewCond(&rf.mu)
	DPrintf("[%d] 开始append,当前Lastindex=%d state:{%v} term:{%d}\n",rf.me,rf.lastlog().Index,rf.state,rf.currentTerm)
	for i:=0;i<len(rf.peers);i++ {
		if i==rf.me {
			continue
		}
		rf.mu.Lock()
		args:=AppendEntriesArgs {
			Term:term,
			Leaderid:rf.me,
			Leadercommit:rf.commitindex}
		
		nextindex:=rf.nextindex[i]
		if nextindex<=0 {
			nextindex=1
		}

		args.Prelogindex=nextindex-1;
		args.Prelogterm=rf.log[args.Prelogindex].Term
		for j:=nextindex;j<=rf.lastlog().Index;j++ {
			args.Entries=append(args.Entries,rf.log[j])
		}
		reply:=AppendEntriesReply{}
		rf.mu.Unlock()

		//并行发送appendRPC
		go func(p int,Args AppendEntriesArgs,Reply AppendEntriesReply){
			ok:=rf.sendappendEntries(p,&Args,&Reply)
			DPrintf("[%d] 已发送给%d state:{%v} term:{%d}\n",rf.me,p,rf.state,rf.currentTerm)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer cond.Broadcast()

			finished++

			if !ok{
				return
			}

			if term!=rf.currentTerm{
				return
			}

			if Reply.Term>rf.currentTerm {
				rf.Convertfollower(Reply.Term)
				rf.updatelasttime()
				return
			}
			if rf.state!="leader" {
				return
			}
 //append成功，意味着直接更新到了旧lastlogindex：Prelogindex+len(Entries)
			if Reply.Success==true {
				vote++
				rf.matchindex[p]=Args.Prelogindex+len(Args.Entries)
				rf.nextindex[p]=rf.matchindex[p]+1
			}
 //append失败，快速回滚
			if Reply.Success==false {
				//1:x.term==-1 :			
				//Follower 日志少，下一次从 XLen 开始
				if Reply.Xterm==-1 {
					rf.nextindex[p]=Reply.Xlen
				}
				if Reply.Xterm!=-1{
					Next:=-1
					for j:=Args.Prelogindex;j>=0;j--{
						if rf.log[j].Term==Reply.Xterm{
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
						rf.nextindex[p]=Reply.Xindex
					}
				}
			}			
		}(i,args,reply)
	}
	rf.mu.Lock()
	for vote<=len(rf.peers)/2 && finished<len(rf.peers){
		cond.Wait()
	}
	rf.mu.Unlock()
	//Leader 开始提交任务
	rf.Leadercommit()
 }

func (rf *Raft) Leadercommit(){
	//只能提交自己周期的任务，其他周期的任务顺带提交
	rf.mu.Lock()
  defer rf.mu.Unlock()
	for i:=rf.commitindex+1;i<=rf.lastlog().Index;i++ {
		if rf.log[i].Term!=rf.currentTerm {
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
	rf.apply()
 }

func (rf *Raft) apply(){
	// for j:=rf.lastapplied+1;j<=rf.commitindex;j++ {
	// 	applymsg:=ApplyMsg {
	// 		CommandValid :true,
	// 		Command: rf.log[j].Command,
	// 		CommandIndex: rf.log[j].Index,
	// 	}

	// 	rf.applyChan<-applymsg
	// 	rf.lastapplied=max(rf.lastapplied,j)
	// 	DPrintf("[%d] state:{%v} term:{%d} 应用到%d\n",rf.me,rf.state,rf.currentTerm,rf.lastapplied)
	// }
	for rf.commitindex > rf.lastapplied && rf.lastlog().Index > rf.lastapplied {
		rf.lastapplied++
		applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastapplied].Command,
				CommandIndex: rf.lastapplied,
		}
		rf.mu.Unlock()
		rf.applyChan <- applyMsg
		rf.mu.Lock()
		DPrintf("[%d] state:{%v} term:{%d} 应用到%d\n",rf.me,rf.state,rf.currentTerm,rf.lastapplied)
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

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
