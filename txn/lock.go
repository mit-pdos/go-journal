package txn

import (
	"fmt"
	"sync"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/util"
)

//
// A sharded map from address to sleeplock's
//

const NSHARD uint64 = 43

type sleepLock struct {
	mu      *sync.Mutex
	cond    *sync.Cond
	holder  TransId
	nwaiter uint64
}

type lockShard struct {
	mu    *sync.Mutex
	addrs *buf.AddrMap
}

func mkLockShard() *lockShard {
	a := &lockShard{
		mu:    new(sync.Mutex),
		addrs: buf.MkAddrMap(),
	}
	return a
}

// Lookup addr and return locked sleepLock for addr.  Lock ordering:
// first lmap lock, then lock sleepLock
func (lmap *lockShard) lookup(addr buf.Addr) *sleepLock {
	var sl *sleepLock
	lmap.mu.Lock()
	e := lmap.addrs.Lookup(addr)
	if e == nil {
		lmap.mu.Unlock()
		return sl
	}
	sl = e.(*sleepLock)
	sl.mu.Lock()
	lmap.mu.Unlock()
	return sl
}

// Atomically lookup and, if not present, add a sleeplock
// struct. Return locked sleepLock.
func (lmap *lockShard) lookupadd(addr buf.Addr) *sleepLock {
	lmap.mu.Lock()
	e := lmap.addrs.Lookup(addr)
	if e == nil {
		l := new(sync.Mutex)
		sleepLock := &sleepLock{holder: 0, mu: l, cond: sync.NewCond(l)}
		lmap.addrs.Insert(addr, sleepLock)
	}
	e = lmap.addrs.Lookup(addr)
	sl := e.(*sleepLock)
	sl.mu.Lock()
	lmap.mu.Unlock()
	return sl
}

// Atomically lookup addr and del sleeplock for addr, if not in use
func (lmap *lockShard) lookupdel(addr buf.Addr) *sleepLock {
	lmap.mu.Lock()
	e := lmap.addrs.Lookup(addr)
	if e == nil {
		util.DPrintf(5, "already deleted addr %v\n", addr)
		lmap.mu.Unlock()
		return nil
	}
	sl := e.(*sleepLock)
	sl.mu.Lock()
	if sl.holder == 0 && sl.nwaiter == 0 {
		util.DPrintf(5, "del addr %v\n", addr)
		lmap.addrs.Del(addr)
	} else {
		util.DPrintf(5, "don't del addr %v\n", addr)
	}
	sl.mu.Unlock()
	lmap.mu.Unlock()
	return sl
}

func (lmap *lockShard) acquire(addr buf.Addr, id TransId) {
	sleepLock := lmap.lookupadd(addr)
	util.DPrintf(5, "%d: acquire: %v\n", id, addr)
	for sleepLock.holder != 0 {
		sleepLock.nwaiter += 1
		sleepLock.cond.Wait()
		sleepLock.nwaiter -= 1
	}
	sleepLock.holder = id
	sleepLock.mu.Unlock()
	util.DPrintf(5, "%d: acquire -> %v\n", id, addr)
}

// release sleeplock, but cannot delete it from addr map right here,
// because we need the lock for the addr map.  instead, release hints
// if the lock can be deleted.
func (lmap *lockShard) dorelease(addr buf.Addr, id TransId) bool {
	var delete bool = true
	sleepLock := lmap.lookup(addr)
	util.DPrintf(5, "%d: dorelease: %v\n", id, addr)
	if sleepLock == nil {
		panic(fmt.Sprintf("dorelease: %d %v", id, addr))
	}
	if sleepLock.holder != id {
		panic(fmt.Sprintf("dorelease: %v %d %d", addr, sleepLock.holder, id))
	}
	sleepLock.holder = 0
	if sleepLock.nwaiter > 0 {
		delete = false
		sleepLock.cond.Signal()
	}
	util.DPrintf(5, "%d: dorelease %v -> %v\n", id, addr, delete)
	sleepLock.mu.Unlock()
	return delete
}

func (lmap *lockShard) release(addr buf.Addr, id TransId) {
	del := lmap.dorelease(addr, id)
	if del {
		lmap.lookupdel(addr)
	}
}

type lockMap struct {
	shards []*lockShard
}

func index(addr buf.Addr) uint64 {
	i := addr.Flatid()
	return i % NSHARD
}

func mkLockMap() *lockMap {
	shards := make([]*lockShard, NSHARD)
	for i := uint64(0); i < NSHARD; i++ {
		shards[i] = mkLockShard()
	}
	a := &lockMap{
		shards: shards,
	}
	return a
}

func (lmap *lockMap) acquire(addr buf.Addr, id TransId) {
	shard := lmap.shards[index(addr)]
	shard.acquire(addr, id)
}

func (lmap *lockMap) release(addr buf.Addr, id TransId) {
	shard := lmap.shards[index(addr)]
	shard.release(addr, id)
}
