package txn

import (
	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/util"

	"sync"
)

//
// A map from address to sleeplock
//
// XXX should delete entries
//

type sleepLock struct {
	mu      *sync.Mutex
	cond    *sync.Cond
	holder  TransId
	nwaiter uint64
}

type lockMap struct {
	mu    *sync.Mutex
	addrs *buf.AddrMap
}

func mkLockMap() *lockMap {
	a := &lockMap{
		mu:    new(sync.Mutex),
		addrs: buf.MkAddrMap(),
	}
	return a
}

func (lmap *lockMap) Len() uint64 {
	n := lmap.addrs.Len()
	return n
}

// atomically lookup and add addr
func (lmap *lockMap) lookupadd(addr buf.Addr) *sleepLock {
	lmap.mu.Lock()
	e := lmap.addrs.Lookup(addr)
	if e == nil {
		l := new(sync.Mutex)
		sleepLock := &sleepLock{holder: 0, mu: l, cond: sync.NewCond(l)}
		lmap.addrs.Insert(addr, sleepLock)
	}
	e = lmap.addrs.Lookup(addr)
	sleepLock := e.(*sleepLock)
	lmap.mu.Unlock()
	return sleepLock
}

// atomically lookup and del if not in use
func (lmap *lockMap) lookupdel(addr buf.Addr) *sleepLock {
	lmap.mu.Lock()
	e := lmap.addrs.Lookup(addr)
	if e == nil {
		util.DPrintf(5, "already deleted addr %v\n", addr)
		lmap.mu.Unlock()
		return nil
	}
	sleepLock := e.(*sleepLock)
	sleepLock.mu.Lock()
	del := sleepLock.holder == 0
	sleepLock.mu.Unlock()
	if del {
		lmap.addrs.Del(addr)
	} else {
		util.DPrintf(5, "don't del addr %v\n", addr)
	}
	lmap.mu.Unlock()
	return sleepLock
}

func (lmap *lockMap) isLocked(addr buf.Addr, id TransId) bool {
	locked := false
	sleepLock := lmap.lookupadd(addr)
	sleepLock.mu.Lock()
	if sleepLock.holder == id {
		locked = true
	}
	sleepLock.mu.Unlock()
	return locked
}

func (lmap *lockMap) acquire(addr buf.Addr, id TransId) {
	util.DPrintf(15, "%d: acquire: %v\n", id, addr)
	sleepLock := lmap.lookupadd(addr)
	sleepLock.mu.Lock()
	for sleepLock.holder != 0 {
		sleepLock.nwaiter += 1
		sleepLock.cond.Wait()
		sleepLock.nwaiter -= 1
	}
	sleepLock.holder = id
	sleepLock.mu.Unlock()
	util.DPrintf(5, "%d: acquire -> %v\n", id, addr)
}

func (lmap *lockMap) dorelease(addr buf.Addr, id TransId) bool {
	var delete bool = true
	util.DPrintf(15, "%d: dorelease: %v\n", id, addr)
	sleepLock := lmap.lookupadd(addr)
	sleepLock.mu.Lock()
	if sleepLock.holder != id {
		panic("release")
	}
	sleepLock.holder = 0
	if sleepLock.nwaiter > 0 {
		delete = false
		sleepLock.cond.Signal()
	}
	sleepLock.mu.Unlock()
	util.DPrintf(15, "%d: dorelease %v -> %v\n", id, addr, delete)
	return delete
}

func (lmap *lockMap) release(addr buf.Addr, id TransId) {
	del := lmap.dorelease(addr, id)
	if del {
		lmap.lookupdel(addr)
	}
}

// release all blocks held by txn
func (lmap *lockMap) releaseTxn(id TransId) {
	lmap.mu.Lock()
	util.DPrintf(15, "%d: releaseTxn %d\n", id, lmap.Len())
	var addrs = make([]buf.Addr, 0)
	lmap.addrs.Apply(func(a buf.Addr, e interface{}) {
		sleepLock := e.(*sleepLock)
		sleepLock.mu.Lock()
		if sleepLock.holder == id {
			addrs = append(addrs, a)
		}
		sleepLock.mu.Unlock()
	})
	lmap.mu.Unlock()
	for _, a := range addrs {
		lmap.release(a, id)
	}
}
