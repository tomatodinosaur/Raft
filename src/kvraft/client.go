package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqid int
	leaderid int
	clientid int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientid=nrand()
	ck.leaderid=int (nrand()%int64(len(servers)))
	return ck
}


func (ck *Clerk) Get(key string) string {
	ck.seqid++
	args:=GetArgs{
		Key:key,
		Clientid:ck.clientid,
		Seqid:ck.seqid}
	serverId:=ck.leaderid
	for {
		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderid = serverId
				DPrintf("[%d] %d:Get wrong Err:%v\n",ck.clientid,ck.seqid,reply.Err)
				return ""
			} else if reply.Err == OK {
				ck.leaderid = serverId
				DPrintf("[%d] %d:Get success \n",ck.clientid,ck.seqid)
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				DPrintf("[%d] %d:Get wrong Err:%v\n",ck.clientid,ck.seqid,reply.Err)
				continue
			}
		}
		// 节点发生crash等原因
		serverId = (serverId + 1) % len(ck.servers)
	}
}


func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqid++
	serverId := ck.leaderid
	args := PutAppendArgs{
		Key: key, 
		Value: value, 
		Op: op, 
		Clientid: ck.clientid, 
		Seqid: ck.seqid}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderid = serverId
				DPrintf("[%d] %d:%v success \n",ck.clientid,ck.seqid,op)
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				DPrintf("[%d] %d:%v wrong Err:%v\n",ck.clientid,ck.seqid,op,reply.Err)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
