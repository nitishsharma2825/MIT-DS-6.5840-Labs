package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Type      string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db             map[string]string
	clientRequests map[int64]Op

	responseCh  map[int]chan Op
	lastApplied int
}

func (kv *KVServer) lastOperation(clientId int64) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op, ok := kv.clientRequests[clientId]; ok {
		return op.RequestId
	}
	return -1
}

func (kv *KVServer) fetchDuplicateEntry(clientId int64, requestId int) (bool, string) {
	op, ok := kv.clientRequests[clientId]
	if !ok || op.RequestId < requestId {
		return false, ""
	}

	return true, op.Value
}

func (kv *KVServer) checkChannel(index int) chan Op {
	_, ok := kv.responseCh[index]
	if !ok {
		kv.responseCh[index] = make(chan Op, 1)
	}

	return kv.responseCh[index]
}

func (kv *KVServer) sameOps(a Op, b Op) bool {
	return a.ClientId == b.ClientId &&
		a.RequestId == b.RequestId &&
		a.Key == b.Key &&
		a.Type == b.Type
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// check for idempotency
	if kv.lastOperation(args.ClientId) >= args.RequestId {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.db[args.Key]
		reply.Err = OK
		return
	}

	reply.Err = ErrWrongLeader

	op := Op{
		Key:       args.Key,
		Value:     "",
		Type:      "get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return
	}

	kv.mu.Lock()
	ch := kv.checkChannel(index)
	kv.mu.Unlock()

	// wait for the message to be committed in raft log
	select {
	case operation := <-ch:
		if !kv.sameOps(operation, op) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Value = operation.Value
		reply.Err = OK
	case <-time.After(time.Duration(500) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// check for idempotency
	if kv.lastOperation(args.ClientId) >= args.RequestId {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = OK
		return
	}

	reply.Err = ErrWrongLeader

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      "put",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return
	}

	kv.mu.Lock()
	ch := kv.checkChannel(index)
	kv.mu.Unlock()

	// wait for the message to be committed in raft log
	select {
	case operation := <-ch:
		if !kv.sameOps(operation, op) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
	case <-time.After(time.Duration(500) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// check for idempotency
	if kv.lastOperation(args.ClientId) >= args.RequestId {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = OK
		return
	}

	reply.Err = ErrWrongLeader

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      "append",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return
	}

	kv.mu.Lock()
	ch := kv.checkChannel(index)
	kv.mu.Unlock()

	// wait for the message to be committed in raft log
	select {
	case operation := <-ch:
		if !kv.sameOps(operation, op) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
	case <-time.After(time.Duration(500) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) receiveUpdates() {
	for response := range kv.applyCh {
		if response.CommandValid {
			kv.mu.Lock()

			op := response.Command.(Op)

			if kv.lastApplied >= response.CommandIndex {
				kv.mu.Unlock()
				continue
			}

			// check if this request is already processed
			ok, val := kv.fetchDuplicateEntry(op.ClientId, op.RequestId)

			ch := kv.checkChannel(response.CommandIndex)

			if ok {
				op.Value = val
			} else {
				if op.Type == "get" {
					op.Value = kv.db[op.Key]
					kv.clientRequests[op.ClientId] = op
				} else if op.Type == "put" {
					kv.db[op.Key] = op.Value
					op.Value = kv.db[op.Key]
					kv.clientRequests[op.ClientId] = op
				} else if op.Type == "append" {
					kv.db[op.Key] += op.Value
					op.Value = kv.db[op.Key]
					kv.clientRequests[op.ClientId] = op
				}
			}

			kv.lastApplied = response.CommandIndex

			kv.mu.Unlock()

			ch <- op
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
	kv.db = make(map[string]string)
	kv.clientRequests = make(map[int64]Op)

	kv.responseCh = make(map[int]chan Op)

	kv.lastApplied = 0

	go kv.receiveUpdates()

	return kv
}
