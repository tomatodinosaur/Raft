package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // raft服务层传来的Index
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate     int // snapshot if log grows this big
	clientmaxseq     map[int64]int
	waitRaft         map[int]chan Op
	DataBase         map[string]string
	lastIncludeIndex int
}

func (kv *KVServer) repeat(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, ok := kv.clientmaxseq[clientId]
	if !ok {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) Listen() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				if msg.CommandIndex <= kv.lastIncludeIndex {
					return
				}
				index := msg.CommandIndex
				op := msg.Command.(Op)
				if !kv.repeat(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.DataBase[op.Key] = op.Value
					case "Append":
						kv.DataBase[op.Key] += op.Value
					}
					kv.clientmaxseq[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				}

				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				//汇报给get/pushappend chan，任务已完成
				kv.mu.Lock()
				ch, ok := kv.waitRaft[index]
				if !ok {
					kv.waitRaft[index] = make(chan Op, 1)
					ch = kv.waitRaft[index]
				}
				kv.mu.Unlock()
				ch <- op
			}
			if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.DecodeSnapShot(msg.Snapshot)
					kv.lastIncludeIndex = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		OpType:   "Get",
		Key:      args.Key,
		SeqId:    args.Seqid,
		ClientId: args.Clientid}
	index, _, _ := kv.rf.Start(op)
	//创建命令通道
	kv.mu.Lock()
	ch, ok := kv.waitRaft[index]
	if !ok {
		kv.waitRaft[index] = make(chan Op, 1)
		ch = kv.waitRaft[index]
	}
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.waitRaft, op.Index)
		kv.mu.Unlock()
	}()
	select {
	case raft_Index_op := <-ch:
		if op.ClientId != raft_Index_op.ClientId || op.SeqId != raft_Index_op.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.DataBase[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-time.After(time.Millisecond * 100):
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		SeqId:    args.Seqid,
		ClientId: args.Clientid}
	index, _, _ := kv.rf.Start(op)
	//创建命令通道
	kv.mu.Lock()
	ch, ok := kv.waitRaft[index]
	if !ok {
		kv.waitRaft[index] = make(chan Op, 1)
		ch = kv.waitRaft[index]
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitRaft, op.Index)
		kv.mu.Unlock()
	}()

	select {
	case raft_Index_op := <-ch:
		if op.ClientId != raft_Index_op.ClientId || op.SeqId != raft_Index_op.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-time.After(time.Millisecond * 100):
		reply.Err = ErrWrongLeader
	}

}
func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var clientmaxseq map[int64]int

	if d.Decode(&kvPersist) == nil && d.Decode(&clientmaxseq) == nil {
		kv.DataBase = kvPersist
		kv.clientmaxseq = clientmaxseq
	} else {
		DPrintf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	}
}

// PersistSnapShot 持久化快照对应的map
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.DataBase)
	e.Encode(kv.clientmaxseq)
	data := w.Bytes()
	return data
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
	kv.clientmaxseq = make(map[int64]int)
	kv.DataBase = make(map[string]string)
	kv.waitRaft = make(map[int]chan Op)

	kv.lastIncludeIndex = -1

	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.Listen()
	return kv
}
