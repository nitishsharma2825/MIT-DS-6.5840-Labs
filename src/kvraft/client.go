package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int
	clientId  int64
	requestId int
	mu        sync.Mutex
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
	ck.leaderId = int(nrand()) % len(servers)
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) getRequestId() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestId++
	return ck.requestId
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	// Generate a unique request ID
	requestID := ck.getRequestId()

	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: requestID,
	}

	reply := GetReply{
		Err:   ErrWrongLeader,
		Value: "",
	}

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = int(nrand()) % len(ck.servers)
			time.Sleep(time.Duration(100) * time.Microsecond)
			continue
		}

		if reply.Err == OK {
			return reply.Value
		}

		if reply.Err == ErrNoKey {
			break
		}
	}

	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// Generate a unique request ID
	requestID := ck.getRequestId()

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		ClientId:  ck.clientId,
		RequestId: requestID,
	}

	reply := PutAppendReply{
		Err: ErrWrongLeader,
	}

	for {
		ok := ck.servers[ck.leaderId].Call("KVServer."+op, &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = int(nrand()) % len(ck.servers)
			time.Sleep(time.Duration(100) * time.Microsecond)
			continue
		}

		if reply.Err == OK {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
