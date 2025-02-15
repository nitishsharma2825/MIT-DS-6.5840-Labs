package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db             map[int]map[string]string // shard -> key-value
	clientRequests map[int64]int             // clientId -> requestId

	responseCh  map[int]chan Op
	lastApplied int

	serveShards map[int]string // shard -> status, shards this replica group is managing with its status

	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config

	mck *shardctrler.Clerk
}

func (kv *ShardKV) checkDuplicateEntry(clientId int64, requestId int) bool {
	reqId, ok := kv.clientRequests[clientId]
	if !ok || reqId < requestId {
		return false
	}

	return true
}

func (kv *ShardKV) checkChannel(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.responseCh[index]
	if !ok {
		kv.responseCh[index] = make(chan Op, 1)
	}

	return kv.responseCh[index]
}

func (kv *ShardKV) sameOps(a Op, b Op) bool {
	return a.ClientId == b.ClientId &&
		a.RequestId == b.RequestId &&
		a.Type == b.Type
}

// check if this replica manages shard for this key
func (kv *ShardKV) handleKey(key string) bool {
	shard := key2shard(key)
	if val, ok := kv.serveShards[shard]; !ok || val != "serve" {
		DPrintf("[%d %d] WRONG_GROUP key [%s] shard [%d] but my Shards status %v", kv.gid, kv.me, key, shard, kv.serveShards)
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	if !kv.handleKey(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	shard := key2shard(args.Key)

	isDup := kv.checkDuplicateEntry(args.ClientId, args.RequestId)
	if isDup {
		reply.Value = kv.db[shard][args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	command := Op{
		Key:       args.Key,
		Value:     "",
		Type:      "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.checkChannel(index)

	select {
	case op := <-ch:
		if !kv.sameOps(op, command) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Value = op.Value
		reply.Err = OK
	case <-time.After(time.Duration(300) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if !kv.handleKey(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	isDup := kv.checkDuplicateEntry(args.ClientId, args.RequestId)
	if isDup {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	command := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.checkChannel(index)

	select {
	case op := <-ch:
		if !kv.sameOps(op, command) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
	case <-time.After(time.Duration(300) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) takeSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// need to take a snapshot?
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		encoder := labgob.NewEncoder(w)
		encoder.Encode(kv.db)
		encoder.Encode(kv.clientRequests)
		encoder.Encode(kv.currentConfig)
		encoder.Encode(kv.serveShards)

		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.lastApplied, snapshot)
	}
}

func (kv *ShardKV) restoreState(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(snapshot) == 0 || kv.maxraftstate == -1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(r)

	var tmpDb map[int]map[string]string
	var tmpClientRequests map[int64]int
	var tmpCurrentConfig shardctrler.Config
	var tmpServeShards map[int]string

	if decoder.Decode(&tmpDb) != nil ||
		decoder.Decode(&tmpClientRequests) != nil ||
		decoder.Decode(&tmpCurrentConfig) != nil ||
		decoder.Decode(&tmpServeShards) != nil {
		fmt.Println("Error during restoring state from snapshot")
	} else {
		kv.db = tmpDb
		kv.clientRequests = tmpClientRequests
		kv.currentConfig = tmpCurrentConfig
		kv.serveShards = tmpServeShards
		kv.lastConfig = kv.mck.Query(kv.currentConfig.Num - 1)
	}
}

func (kv *ShardKV) receiveUpdates() {
	for response := range kv.applyCh {
		if response.CommandValid {

			kv.mu.Lock()

			if response.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = response.CommandIndex

			kv.mu.Unlock()

			if cfg, ok := response.Command.(shardctrler.Config); ok {
				kv.handleConfigChange(cfg)
			} else if reply, ok := response.Command.(MigrateReply); ok {
				kv.handleMigration(&reply)
			} else {

				op := response.Command.(Op)
				shard := key2shard(op.Key)

				kv.mu.Lock()
				if !kv.handleKey(op.Key) {
					kv.mu.Unlock()
					continue
				}
				kv.mu.Unlock()

				ch := kv.checkChannel(response.CommandIndex)

				kv.mu.Lock()

				_, ok := kv.db[shard]
				if !ok {
					kv.db[shard] = make(map[string]string)
				}

				// check if this request is already processed
				isDup := kv.checkDuplicateEntry(op.ClientId, op.RequestId)

				if isDup {
					op.Value = kv.db[shard][op.Key]
				} else {
					if op.Type == "Get" {
						op.Value = kv.db[shard][op.Key]
					} else if op.Type == "Put" {
						kv.db[shard][op.Key] = op.Value
						op.Value = kv.db[shard][op.Key]
					} else if op.Type == "Append" {
						kv.db[shard][op.Key] += op.Value
						op.Value = kv.db[shard][op.Key]
					}
					kv.clientRequests[op.ClientId] = op.RequestId
				}

				kv.mu.Unlock()
				ch <- op
			}
		}
		if response.SnapshotValid {
			kv.restoreState(response.Snapshot)
			kv.mu.Lock()
			kv.lastApplied = response.SnapshotIndex
			kv.mu.Unlock()
		}
		kv.takeSnapshot()
	}
}

func (kv *ShardKV) pollConfigChanges() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()

			canPullLatestConfig := true

			// at first, this will be empty so we can pull starting config
			for _, status := range kv.serveShards {
				if status != "serve" {
					canPullLatestConfig = false
					break
				}
			}

			if canPullLatestConfig {
				nextConfigNum := kv.currentConfig.Num + 1
				newConfig := kv.mck.Query(nextConfigNum)
				if newConfig.Num == nextConfigNum {
					// commit in log before applying new config
					kv.rf.Start(newConfig)
				}
			}
			kv.mu.Unlock()
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) handleConfigChange(newConfig shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// DPrintf("[%d %d] Inside handle config change before, my current config %v", kv.gid, kv.me, kv.serveShards)

	if newConfig.Num <= kv.currentConfig.Num {
		return
	}

	kv.serveShards = make(map[int]string)

	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			if newConfig.Num == 1 {
				// first config, serve immediately
				kv.serveShards[shard] = "serve"
			} else {
				// else mark all shards in wait mode
				kv.serveShards[shard] = "wait"
			}
		}
	}

	// can start serving for those shards which were part of old config
	// for other shards in config, need to make sure data is present, therefore they will be in wait mode
	for shard, gid := range kv.currentConfig.Shards {
		_, present := kv.serveShards[shard]
		if gid == kv.gid && present {
			kv.serveShards[shard] = "serve"
		}
	}

	kv.lastConfig = kv.currentConfig
	kv.currentConfig = newConfig

	// DPrintf("[%d %d] Inside handle config change after, my current config %v", kv.gid, kv.me, kv.serveShards)
}

func (kv *ShardKV) handleMigration(reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reply.ConfigNum != kv.currentConfig.Num-1 {
		return
	}

	// DPrintf("[%d %d] Inside handle migration, my current config %v", kv.gid, kv.me, kv.serveShards)

	for shard, kvData := range reply.Data {
		for k, v := range kvData {
			if _, ok := kv.db[shard]; !ok {
				kv.db[shard] = make(map[string]string)
			}
			kv.db[shard][k] = v
		}
		kv.serveShards[shard] = "serve"
	}

	for k, v := range reply.CachedRequests {
		if _, ok := kv.clientRequests[k]; !ok || kv.clientRequests[k] < v {
			kv.clientRequests[k] = v
		}
	}
}

func (kv *ShardKV) pullData() {
	for {

		_, isLeader := kv.rf.GetState()

		if isLeader {
			kv.mu.Lock()

			// from each replica(GID), which shards are needed?
			requestData := make(map[int][]int)

			// DPrintf("[%d %d] Inside Pulling data, my current config %v", kv.gid, kv.me, kv.serveShards)

			// for every shard which is in waitStatus, ask the replica group to send data
			// use the last config (not current config) to find the replica group for shards
			for shard, status := range kv.serveShards {
				if status == "wait" {
					gid := kv.lastConfig.Shards[shard]
					if _, ok := requestData[gid]; !ok {
						requestData[gid] = make([]int, 0)
					}
					requestData[gid] = append(requestData[gid], shard)
				}
			}

			var wg sync.WaitGroup
			for gid, shards := range requestData {
				args := MigrateArgs{
					Shards:    shards,
					ConfigNum: kv.currentConfig.Num - 1,
				}
				wg.Add(1)
				go kv.sendMigrateRPC(&wg, &args, kv.lastConfig.Groups[gid])
			}

			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *ShardKV) sendMigrateRPC(wg *sync.WaitGroup, args *MigrateArgs, servers []string) {
	defer wg.Done()
	for _, server := range servers {
		// get server endpoint to call
		srv := kv.make_end(server)
		reply := MigrateReply{}
		ok := srv.Call("ShardKV.Migrate", args, &reply)
		if ok && reply.Err == OK {
			// commit to log before merging data, handled in receiveUpdates and handleMigration functions
			kv.rf.Start(reply)
		}
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.currentConfig.Num {
		reply.Err = ErrWrongGroup
		return
	}

	reply.Data = make(map[int]map[string]string)
	reply.CachedRequests = make(map[int64]int)
	reply.ConfigNum = args.ConfigNum

	for _, shard := range args.Shards {
		reply.Data[shard] = make(map[string]string)
		for k, v := range kv.db[shard] {
			reply.Data[shard][k] = v
		}
	}

	for k, v := range kv.clientRequests {
		reply.CachedRequests[k] = v
	}

	reply.Err = OK
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[int]map[string]string)
	kv.responseCh = make(map[int]chan Op)
	kv.clientRequests = make(map[int64]int)
	kv.serveShards = make(map[int]string)
	kv.lastApplied = 0
	kv.currentConfig = kv.mck.Query(0)
	kv.lastConfig = kv.mck.Query(0)

	// DPrintf("[%d %d] Starting with config %v", kv.gid, kv.me, kv.currentConfig.Shards)

	kv.restoreState(persister.ReadSnapshot())

	go kv.receiveUpdates()

	go kv.pollConfigChanges()

	go kv.pullData()

	return kv
}
