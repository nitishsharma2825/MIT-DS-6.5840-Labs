package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type CacheValue struct {
	requestID int64
	value     string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	db             map[string]string
	clientRequests map[int64]*CacheValue
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if client ID exists
	value, exists := kv.clientRequests[args.ClientID]
	if exists {
		// check for duplicate request
		if value.requestID >= args.RequestID {
			reply.Value = ""
		} else {
			value.requestID = args.RequestID
			value.value = ""
			kv.db[args.Key] = args.Value
			reply.Value = value.value
		}
		return
	}

	// if client ID does not exist
	kv.clientRequests[args.ClientID] = &CacheValue{
		requestID: args.RequestID,
		value:     "",
	}

	kv.db[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if client ID exists
	value, exists := kv.clientRequests[args.ClientID]
	if exists {
		// check for duplicate request
		if value.requestID >= args.RequestID {
			reply.Value = value.value
		} else {
			oldval := kv.db[args.Key]
			newval := oldval + args.Value
			kv.db[args.Key] = newval
			value.value = oldval
			value.requestID = args.RequestID
			reply.Value = value.value
		}
		return
	}

	// if client ID does not exist
	oldval := kv.db[args.Key]
	newval := oldval + args.Value
	kv.db[args.Key] = newval

	kv.clientRequests[args.ClientID] = &CacheValue{
		requestID: args.RequestID,
		value:     oldval,
	}

	reply.Value = oldval
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.clientRequests = make(map[int64]*CacheValue)
	kv.mu = sync.Mutex{}

	return kv
}
