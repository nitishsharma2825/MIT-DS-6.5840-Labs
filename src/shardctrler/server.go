package shardctrler

import (
	"sync"

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

	currentConfigNo int
	noOfGroups      int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Config Config
	Type   string
}

func (sc *ShardCtrler) getNewShardMapping(groups int) [NShards]int {
	result := [NShards]int{}

	curGroup := 0
	for i := 0; i < NShards; i++ {
		result[i] = curGroup % groups
		curGroup++
	}

	return result
}

func (sc *ShardCtrler) getNewGroupMapping(removedGroups []int) map[int][]string {
	result := map[int][]string{}

	for gid, servers := range sc.configs[sc.currentConfigNo].Groups {
		contains := false
		for _, val := range removedGroups {
			if val == gid {
				contains = true
				break
			}
		}

		if !contains {
			result[gid] = servers
		}
	}

	return result
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	newShardMapping := sc.getNewShardMapping(len(args.Servers))

	newConfig := Config{
		Num:    sc.currentConfigNo + 1,
		Groups: args.Servers,
		Shards: newShardMapping,
	}

	op := Op{
		Config: newConfig,
		Type:   "join",
	}

	// Need to commit to raft
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	commitMsg := <-sc.applyCh

	if commitMsg.CommandValid && commitMsg.CommandIndex == index {
		sc.currentConfigNo++
		sc.noOfGroups = len(args.Servers)
		sc.configs = append(sc.configs, newConfig)
		reply.Err = OK
		return
	}

	reply.WrongLeader = true
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	newGroupMapping := sc.getNewGroupMapping(args.GIDs)
	newShardMapping := sc.getNewShardMapping(len(newGroupMapping))

	newConfig := Config{
		Num:    sc.currentConfigNo + 1,
		Groups: newGroupMapping,
		Shards: newShardMapping,
	}

	op := Op{
		Config: newConfig,
		Type:   "leave",
	}

	// Need to commit to raft
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	commitMsg := <-sc.applyCh

	if commitMsg.CommandValid && commitMsg.CommandIndex == index {
		sc.currentConfigNo++
		sc.noOfGroups = len(newGroupMapping)
		sc.configs = append(sc.configs, newConfig)
		reply.Err = OK
		return
	}

	reply.WrongLeader = true
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	newShardMapping := [NShards]int{}
	for shard, gid := range sc.configs[sc.currentConfigNo].Shards {
		if args.Shard != shard {
			newShardMapping[shard] = gid
			continue
		}

		newShardMapping[shard] = args.GID
	}

	newConfig := Config{
		Num:    sc.currentConfigNo + 1,
		Groups: sc.configs[sc.currentConfigNo].Groups,
		Shards: newShardMapping,
	}

	op := Op{
		Config: newConfig,
		Type:   "move",
	}

	// Need to commit to raft
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	commitMsg := <-sc.applyCh

	if commitMsg.CommandValid && commitMsg.CommandIndex == index {
		sc.currentConfigNo++
		sc.configs = append(sc.configs, newConfig)
		reply.Err = OK
		return
	}

	reply.WrongLeader = true
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	newConfig := Config{
		Num:    sc.currentConfigNo + 1,
		Groups: sc.configs[sc.currentConfigNo].Groups,
		Shards: sc.configs[sc.currentConfigNo].Shards,
	}

	op := Op{
		Config: newConfig,
		Type:   "query",
	}

	// Need to commit to raft
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	commitMsg := <-sc.applyCh

	if commitMsg.CommandValid && commitMsg.CommandIndex == index {
		sc.currentConfigNo++
		sc.configs = append(sc.configs, newConfig)
		reply.Err = OK
		if args.Num == -1 || args.Num > sc.currentConfigNo-1 {
			reply.Config = sc.configs[sc.currentConfigNo]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		return
	}

	reply.WrongLeader = true

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

	sc.currentConfigNo = 0

	// create an empty config
	sc.configs[0].Num = sc.currentConfigNo
	sc.configs[0].Shards = [NShards]int{}

	op := Op{
		Config: sc.configs[0],
		Type:   "join",
	}

	sc.rf.Start(op)

	<-sc.applyCh

	return sc
}
