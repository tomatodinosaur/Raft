package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
 }

func hash(term int,index int) (Idx int64) {
	Idx=int64(int64(term*10000000)+int64(index))
	return Idx
 }


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string 
	Key string
	Value string
	Clientid int64
	Commandid int
}

type CommandDetail struct{
	Commandid int
	Reply ReplyDetail
}

type ReplyDetail struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
	// Term int //因为需要保证start时候Leader的Index位置的命令 = apply时Index位置的命令
					 //但是由于在这个过程中，有可能发生领导人变化的情况，所以Index位置的日志会发生变化
					 //又因为Index和Term可以唯一确定一条日志，所以需要此Term参数

	//该被应用的command的term,便于RPC handler判断是否为过期请求(之前为leader并且start了,
	//但是后来没有成功commit就变成了follower,导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
	//或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader的commit的log了
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	clientCache    map[int64] CommandDetail     
	DataBase    	 map[string]string            
	replyChan      map[int64]chan ReplyDetail		//构造index/term双重哈希

	// lastApplied    int                          //上一条应用的log的index,防止快照导致回退
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	//1.先判断该命令是否已经被执行过了
	if CommandDetail, ok := kv.clientCache[args.Clientid]; ok {
		 if CommandDetail.Commandid >= args.Commandid {
				//若当前的请求已经被执行过了,那么直接返回结果
				reply.Err = CommandDetail.Reply.Err
				reply.Value = CommandDetail.Reply.Value
				kv.mu.Unlock()
				return
		 }
	}
	kv.mu.Unlock()
	op:=Op{
		Type:"Get",
		Key:args.Key,
		Clientid:args.Clientid,
		Commandid:args.Commandid,
	}
	index,term,isLeader:=kv.rf.Start(op)
	if !isLeader {
		reply.Err=ErrWrongLeader
		return
	}
	Idx:=hash(term,index)
	kv.mu.Lock()
	kv.replyChan[Idx]=make(chan ReplyDetail,1)
	kv.mu.Unlock()
	select {
	case ans:=<-kv.replyChan[Idx]:
		reply.Err=ans.Err
		reply.Value=ans.Value
	case <-time.After(100 * time.Millisecond):
		//超时
		reply.Err=ErrTimeOut
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//存储该client的最后一条命令reply
	newCommandDetail:=CommandDetail{}
	newCommandDetail.Reply=ReplyDetail{reply.Err,reply.Value}
	newCommandDetail.Commandid=args.Commandid
	kv.clientCache[args.Clientid]=newCommandDetail

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	//1.先判断该命令是否已经被执行过了
	if CommandDetail, ok := kv.clientCache[args.Clientid]; ok {
		 if CommandDetail.Commandid == args.Commandid {
				//若当前的请求已经被执行过了,那么直接返回结果
				reply.Err = CommandDetail.Reply.Err
				kv.mu.Unlock()
				return
		 }
	}
	kv.mu.Unlock()
	op:=Op{
		Type:args.Type,
		Key:args.Key,
		Value:args.Value,
		Clientid:args.Clientid,
		Commandid:args.Commandid,
	}
	index,term,isLeader:=kv.rf.Start(op)
	if !isLeader {
		//error Learder
		reply.Err=ErrWrongLeader
		return
	}
	Idx:=hash(term,index)
	kv.mu.Lock()
	kv.replyChan[Idx]=make(chan ReplyDetail,1)
	kv.mu.Unlock()
	select {
	case ans:=<-kv.replyChan[Idx]:
		reply.Err=ans.Err
	case <-time.After(100 * time.Millisecond):
		//超时
		reply.Err=ErrTimeOut
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newCommandDetail:=CommandDetail{}
	newCommandDetail.Reply=ReplyDetail{reply.Err,""}
	newCommandDetail.Commandid=args.Commandid
	kv.clientCache[args.Clientid]=newCommandDetail
}

func (kv *KVServer) Listen(){
	for !kv.killed() {
		select{
		case command:=<-kv.applyCh:
			kv.mu.Lock()
			if command.CommandValid{
				Idx:=hash(command.Term,command.CommandIndex)
				op := command.Command.(Op)
				t:=op.Type
				key:=op.Key
				value:=op.Value
				var reply ReplyDetail
				if CommandDetail, ok := kv.clientCache[op.Clientid]; ok && CommandDetail.Commandid >= op.Commandid {
					kv.mu.Unlock()
					return
			 }
				switch t {
				case "Get":
					reply=kv.handleGet(key)
				case "Append"	:
					reply=kv.handleAppend(key,value)
				case "Put" :
					reply=kv.handlePut(key,value)
			}
				_,ok:=kv.replyChan[Idx]
				if ok{
					kv.replyChan[Idx]<-reply
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) handleGet(key string) ReplyDetail{
	value,ok:=kv.DataBase[key]
	if !ok {
		return ReplyDetail{ErrNoKey,""}
	}
	return  ReplyDetail{OK,value}
}

func (kv *KVServer) handlePut(key string,value string) ReplyDetail{
	kv.DataBase[key]=value
	return  ReplyDetail{OK,""}
}

func (kv *KVServer) handleAppend(key string,value string) ReplyDetail{
	val,ok:=kv.DataBase[key]
	if !ok {
		kv.DataBase[key]=value
	}else {
		kv.DataBase[key]=val+value
	}
	return  ReplyDetail{OK,""}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientCache=make(map[int64] CommandDetail)
	kv.DataBase=make(map[string]string)
	kv.replyChan=make(map[int64]chan ReplyDetail)
	go kv.Listen()
	return kv
}
