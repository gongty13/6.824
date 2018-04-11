package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId     int
	id           int64
	requestCount int
	mu           sync.Mutex
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
	ck.leaderId = 0
	ck.id = nrand()
	ck.requestCount = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//time.Sleep(time.Second/10)
	//println("Getting", key)
	//defer println("got", key)
	ck.mu.Lock()
	ck.requestCount++
	var args GetArgs
	args.Key = key
	args.RequestId = ck.requestCount
	args.ClientId = ck.id
	DPrintln(args)
	defer ck.mu.Unlock()
	for {
		var reply GetReply
		if ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply); ok && !reply.WrongLeader {
			if reply.Err == OK {
				return reply.Value
			}
			break
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//println("================================================putting", key, value, op)
	//defer println("putted", key)
	ck.mu.Lock()
	ck.requestCount++
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.id
	args.RequestId = ck.requestCount
	defer ck.mu.Unlock()
	DPrintln(args)
	//var reply PutAppendReply
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
