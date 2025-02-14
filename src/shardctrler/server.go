package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	lastApplied    int
	clientRequests map[int64]int
	responseCh     map[int]chan Op

	currentConfig Config

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type      string
	ClientId  int64
	RequestId int
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
}

func (sc *ShardCtrler) sameOps(a Op, b Op) bool {
	return a.ClientId == b.ClientId &&
		a.RequestId == b.RequestId &&
		a.Type == b.Type
}

func (sc *ShardCtrler) checkDuplicateEntry(clientId int64, requestId int) bool {
	reqId, ok := sc.clientRequests[clientId]
	if !ok || reqId < requestId {
		return false
	}

	return true
}

func (sc *ShardCtrler) checkChannel(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, ok := sc.responseCh[index]
	if !ok {
		sc.responseCh[index] = make(chan Op, 1)
	}

	return sc.responseCh[index]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()

	// check for idempotency
	isDup := sc.checkDuplicateEntry(args.ClientId, args.RequestId)
	if isDup {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()

	op := Op{
		Type:      "join",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Servers:   args.Servers,
	}

	// Need to commit to raft
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		reply.Err = OK
		return
	}

	ch := sc.checkChannel(index)

	select {
	case replyOp := <-ch:
		if sc.sameOps(op, replyOp) {
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = true
		}
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()

	// check for idempotency
	isDup := sc.checkDuplicateEntry(args.ClientId, args.RequestId)
	if isDup {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()

	op := Op{
		Type:      "leave",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		GIDs:      args.GIDs,
	}

	// Need to commit to raft
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = OK
		return
	}

	ch := sc.checkChannel(index)

	select {
	case replyOp := <-ch:
		if sc.sameOps(op, replyOp) {
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = true
		}
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()

	// check for idempotency
	isDup := sc.checkDuplicateEntry(args.ClientId, args.RequestId)
	if isDup {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()

	op := Op{
		Type:      "move",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	// Need to commit to raft
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = OK
		return
	}

	ch := sc.checkChannel(index)

	select {
	case replyOp := <-ch:
		if sc.sameOps(op, replyOp) {
			reply.WrongLeader = false
		} else {
			reply.WrongLeader = true
		}
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()

	// check for idempotency
	isDup := sc.checkDuplicateEntry(args.ClientId, args.RequestId)
	if isDup {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()

	op := Op{
		Type:      "query",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Num:       args.Num,
	}

	// Need to commit to raft
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = OK
		return
	}

	ch := sc.checkChannel(index)

	select {
	case replyOp := <-ch:
		if sc.sameOps(op, replyOp) {
			reply.WrongLeader = false
			sc.mu.Lock()
			reply.Config = sc.configs[replyOp.Num]
			sc.mu.Unlock()
		} else {
			reply.WrongLeader = true
		}
		reply.Err = OK
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = OK
	}
}

func (sc *ShardCtrler) doJoin(servers map[int][]string) {

	// update the server mapping of the current config
	for key, val := range servers {
		_, ok := sc.currentConfig.Groups[key]
		if !ok {
			sc.currentConfig.Groups[key] = make([]string, 0)
		}

		sc.currentConfig.Groups[key] = append(sc.currentConfig.Groups[key], val...)
	}

	// update the shard mapping of the current config
	sc.balanceLoad()

	// create a new config and add to configs
	sc.saveConfig()
}

func (sc *ShardCtrler) doLeave(gids []int) {
	for _, gid := range gids {
		delete(sc.currentConfig.Groups, gid)
	}

	for i, gid := range sc.currentConfig.Shards {
		for _, val := range gids {
			if gid == val {
				sc.currentConfig.Shards[i] = 0
			}
		}
	}

	sc.balanceLoad()
	sc.saveConfig()
}

func (sc *ShardCtrler) doMove(shard int, gid int) {
	sc.currentConfig.Shards[shard] = gid
	sc.saveConfig()
}

// Divide shards as evenly as possible
// Should move as few shards as possible among the groups
func (sc *ShardCtrler) balanceLoad() {
	activeGIDs := len(sc.currentConfig.Groups)

	// sorted list of GIDs because map iteration order is not deterministic
	keys := make([]int, 0)
	for gid := range sc.currentConfig.Groups {
		keys = append(keys, gid)
	}
	sort.Ints(keys)

	if activeGIDs == 0 {
		return
	}

	avgShardsPerGid := NShards / activeGIDs
	maxShardsPerGid := avgShardsPerGid + 1
	extraShardsPerGid := NShards % activeGIDs

	mappings := make(map[int][]int)
	allot := make([]int, 0)

	// create mapping of gid to shards (GID -> []Shard)
	for i, gid := range sc.currentConfig.Shards {
		if gid == 0 {
			// GID 0 is invalid, meaning shard not assigned GID
			// this can happen if a GID leaves, shard assigned to it is marked 0
			allot = append(allot, i)
		} else {
			_, ok := mappings[gid]
			if !ok {
				mappings[gid] = make([]int, 0)
			}

			mappings[gid] = append(mappings[gid], i)
		}
	}

	// Remove extra shards from each GID
	for _, gid := range keys {
		cutOff := avgShardsPerGid
		if extraShardsPerGid > 0 {
			cutOff = maxShardsPerGid
		}

		if len(mappings[gid]) > cutOff {
			extra := mappings[gid][cutOff:]
			allot = append(allot, extra...)
			mappings[gid] = mappings[gid][:cutOff]
			if cutOff == maxShardsPerGid {
				extraShardsPerGid--
			}
		}
	}

	// Assigned average shards to each GID
	for _, gid := range keys {
		shards := mappings[gid]
		diff := avgShardsPerGid - len(shards)
		if diff >= 0 && len(allot) > 0 {
			if diff > len(allot) {
				diff = len(allot)
			}
			mappings[gid] = append(mappings[gid], allot[:diff]...)
			allot = allot[diff:]
		}
	}

	// Assign remaining extra shards to some GIDs
	for _, gid := range keys {
		if len(allot) == 0 {
			break
		}

		shards := mappings[gid]
		diff := avgShardsPerGid - len(shards)
		if diff == 0 {
			mappings[gid] = append(mappings[gid], allot[0])
			allot = allot[1:]
		}
	}

	// update the shard mapping
	for _, gid := range keys {
		for _, shard := range mappings[gid] {
			sc.currentConfig.Shards[shard] = gid
		}
	}

}

func (sc *ShardCtrler) saveConfig() {
	sc.currentConfig.Num++
	newConfig := Config{
		Num:    sc.currentConfig.Num,
		Shards: sc.currentConfig.Shards,
		Groups: make(map[int][]string),
	}

	for key, val := range sc.currentConfig.Groups {
		newConfig.Groups[key] = val
	}

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) receiveUpdates() {
	for response := range sc.applyCh {
		if response.CommandValid {
			op := response.Command.(Op)

			sc.mu.Lock()

			if sc.lastApplied >= response.CommandIndex {
				sc.mu.Unlock()
				continue
			}

			sc.mu.Unlock()

			ch := sc.checkChannel(response.CommandIndex)

			sc.mu.Lock()

			isDup := sc.checkDuplicateEntry(op.ClientId, op.RequestId)
			if !isDup {
				if op.Type == "join" {
					sc.doJoin(op.Servers)
				} else if op.Type == "leave" {
					sc.doLeave(op.GIDs)
				} else if op.Type == "move" {
					sc.doMove(op.Shard, op.GID)
				} else if op.Type == "query" {
					if op.Num == -1 || op.Num >= len(sc.configs) {
						op.Num = len(sc.configs) - 1
					}
				}
			}

			sc.lastApplied = response.SnapshotIndex
			sc.mu.Unlock()
			ch <- op
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.responseCh = make(map[int]chan Op)
	sc.clientRequests = make(map[int64]int)
	sc.lastApplied = 0
	sc.currentConfig = sc.configs[len(sc.configs)-1]

	go sc.receiveUpdates()

	return sc
}
