package raftkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

const (
	OpAppend = iota
	OpPut
	OpGet
)

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Println(a...)
	}
	return
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Type  int
	Key   string
	Value string
	Id    int
	Cid   int64
}

type OpHeap []Op

func (h OpHeap) Len() int           { return len(h) }
func (h OpHeap) Less(i, j int) bool { return h[i].Id < h[j].Id }
func (h OpHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *OpHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Op))
}

func (h *OpHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type KVServer struct {
	mu        sync.Mutex
	mu_map    sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	data      map[string]string
	maxCommit int
	//pending   map[int64]OpHeap
	opCount  map[int64]int
	commitOp map[int64]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if isLeader {
		reply.WrongLeader = false
		op := Op{Type: OpGet, Key: args.Key, Value: "", Id: args.RequestId, Cid: args.ClientId}
		if v, ok := kv.opCount[args.ClientId]; !ok || op.Id > v {
			kv.mu_map.Lock()
			kv.opCount[args.ClientId] = op.Id
			kv.mu_map.Unlock()
			index, term, isLeader := kv.rf.Start(op)
			DPrintln("start", kv.me, index, term, isLeader)
			if !isLeader {
				reply.WrongLeader = true
				kv.mu_map.Lock()
				kv.opCount[args.ClientId] = op.Id - 1
				kv.mu_map.Unlock()
				DPrintln("not leader any more", kv.me, kv.rf.Detail())
				return
			}
		}
		//time.Sleep(50*time.Millisecond)
		DPrintln("commitI", kv.maxCommit, "me", kv.me, kv.rf.Info(), kv.rf.Detail(), kv.opCount[args.ClientId], op.Id, args.ClientId)
		for kv.maxCommit < kv.rf.Info()+kv.rf.CommandNumInSnap-kv.rf.FakeCommandNumInSnap {
			//DPrintln("commit and Info", kv.maxCommit, kv.rf.Info(), kv.rf.CommandNumInSnap, kv.rf.FakeCommandNumInSnap)
			time.Sleep(time.Millisecond)
			_, isLeader = kv.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				DPrintln("not leader any more", kv.me, kv.rf.Detail())
				return
			}
		}
		//println("arrive", kv.maxCommit, "me:", kv.me)
		if ret, ok := kv.data[args.Key]; ok {
			reply.Err = OK
			reply.Value = ret
			//println("Got", args.Key, ret)
		} else {
			reply.Err = ErrNoKey
		}
		DPrintln("mcommit", kv.maxCommit, "me", kv.me, "cmdnum", kv.rf.Info(), reply.WrongLeader, op.Id, op.Cid, kv.rf.Detail())
		if kv.maxraftstate != -1 && kv.rf.StateSize() >= kv.maxraftstate {
			kv.rf.PrepareSnapShot(kv.data, kv.commitOp)
		}
	} else {
		reply.WrongLeader = true
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if isLeader {
		//println("Imleader", kv.me, isLeader)
		reply.WrongLeader = false
		reply.Err = OK
		op := Op{Type: OpAppend,
			Key:   args.Key,
			Value: args.Value,
			Id:    args.RequestId,
			Cid:   args.ClientId}
		if args.Op == "Put" {
			op.Type = OpPut
		}
		if v, ok := kv.opCount[args.ClientId]; !ok || op.Id > v {
			kv.mu_map.Lock()
			kv.opCount[args.ClientId] = op.Id
			kv.mu_map.Unlock()
			index, term, isLeader := kv.rf.Start(op)
			DPrintln("start", kv.me, index, term, isLeader, op.Id, op.Cid)
			if !isLeader {
				reply.WrongLeader = true
				kv.mu_map.Lock()
				kv.opCount[args.ClientId] = op.Id - 1
				kv.mu_map.Unlock()
				DPrintln("not leader any more", kv.me, kv.rf.Detail())
				return
			}
		}
		//kv.mu_map.Unlock()
		DPrintln("commitI", kv.maxCommit, "me", kv.me, kv.rf.Info(), kv.rf.Detail(), kv.opCount[args.ClientId], op.Id, args.ClientId)
		for kv.maxCommit < kv.rf.Info()+kv.rf.CommandNumInSnap-kv.rf.FakeCommandNumInSnap {
			//DPrintln("commit and Info", kv.maxCommit, kv.rf.Info(), kv.rf.CommandNumInSnap, kv.rf.FakeCommandNumInSnap)
			time.Sleep(time.Millisecond)
			_, isLeader = kv.rf.GetState()
			if !isLeader {
				reply.WrongLeader = true
				kv.mu_map.Lock()
				kv.opCount[args.ClientId] = op.Id - 1
				kv.mu_map.Unlock()
				DPrintln("not leader any more", kv.me, kv.rf.Detail())
				return
			}
		}
		DPrintln("mcommit", kv.maxCommit, "me", kv.me, "cmdnum", kv.rf.Info(), reply.WrongLeader, op.Id, op.Cid, kv.rf.Detail())
		if kv.maxraftstate != -1 && kv.rf.StateSize() >= kv.maxraftstate {
			kv.rf.PrepareSnapShot(kv.data, kv.commitOp)
		}
	} else {
		reply.WrongLeader = true
	}
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
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
	kv.data = make(map[string]string)
	kv.commitOp = make(map[int64]int)
	kv.maxCommit = -1
	go func() {
		for msg := range kv.applyCh {
			kv.apply(&msg)
		}
	}()
	r := bytes.NewBuffer(persister.ReadSnapshot())
	e := labgob.NewDecoder(r)
	e.Decode(&kv.data)
	e.Decode(&kv.commitOp)
	kv.opCount = make(map[int64]int)
	kv.persister = persister
	return kv
}

func (kv *KVServer) apply(msg *raft.ApplyMsg) {

	op, ok := msg.Command.(Op)
	//println("msg", msg, ok, kv.me, msg.Command.(Op).Key,msg.CommandIndex)
	if !ok || msg.CommandIndex <= kv.maxCommit {
		return
	}
	//println("op", op.Type, op.Key, op.Value)

	if kv.maxCommit+1 < msg.CommandIndex {
		r := bytes.NewBuffer(kv.persister.ReadSnapshot())
		e := labgob.NewDecoder(r)
		e.Decode(&kv.data)
		e.Decode(&kv.commitOp)
	}
	if opc, ok := kv.commitOp[op.Cid]; !ok || opc < op.Id {
		kv.commitOp[op.Cid] = op.Id
		switch op.Type {
		case OpAppend:
			kv.data[op.Key] = kv.data[op.Key] + op.Value
		case OpPut:
			kv.data[op.Key] = op.Value
		default:
		}
	}
	//switch op.Type {
	//case OpAppend:
	//	kv.data[op.Key] = kv.data[op.Key] + op.Value
	//case OpPut:
	//	kv.data[op.Key] = op.Value
	//default:
	//}
	kv.maxCommit = msg.CommandIndex
	kv.mu_map.Lock()
	if kv.opCount[op.Cid] < op.Id {
		kv.opCount[op.Cid] = op.Id
	}
	kv.mu_map.Unlock()

}
