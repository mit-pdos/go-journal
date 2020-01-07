package txn

import (
	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/util"

	"sync"
)

type alock struct {
	holder TransId
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

func (lmap *lockMap) isLocked(addr buf.Addr, id TransId) bool {
	locked := false
	lmap.mu.Lock()
	e := lmap.addrs.Lookup(addr)
	if e != nil {
		l := e.(*alock)
		if l.holder == id {
			locked = true
		}
	}
	lmap.mu.Unlock()
	return locked
}

// atomically lookup and add addr
func (lmap *lockMap) lookupadd(addr buf.Addr, id TransId) bool {
	lmap.mu.Lock()
	e := lmap.addrs.Lookup(addr)
	if e == nil {
		alock := &alock{holder: id}
		lmap.addrs.Insert(addr, alock)
		lmap.mu.Unlock()
		return true
	}
	util.DPrintf(5, "LookupAdd already locked %v %v\n", addr, e)
	lmap.mu.Unlock()
	return false
}

func (lmap *lockMap) acquire(addr buf.Addr, id TransId) {
	for {
		if lmap.lookupadd(addr, id) {
			break
		}
		// XXX condition variable?
		continue

	}
	util.DPrintf(5, "%d: acquire: %v\n", id, addr)
}

func (lmap *lockMap) dorelease(addr buf.Addr, id TransId) {
	util.DPrintf(5, "%d: release: %v\n", id, addr)
	e := lmap.addrs.Lookup(addr)
	if e == nil {
		panic("release")
	}
	alock := e.(*alock)
	if alock.holder != id {
		panic("release")
	}
	lmap.addrs.Del(addr)
}

func (lmap *lockMap) release(addr buf.Addr, id TransId) {
	lmap.mu.Lock()
	lmap.dorelease(addr, id)
	lmap.mu.Unlock()
}

// release all blocks held by txn
func (lmap *lockMap) releaseTxn(id TransId) {
	lmap.mu.Lock()
	var addrs = make([]buf.Addr, 0)
	lmap.addrs.Apply(func(a buf.Addr, e interface{}) {
		alock := e.(*alock)
		if alock.holder == id {
			addrs = append(addrs, a)
		}
	})
	for _, a := range addrs {
		lmap.dorelease(a, id)
	}
	lmap.mu.Unlock()
}
