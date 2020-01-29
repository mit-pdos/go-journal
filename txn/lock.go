package txn

import (
	"sync"
)

type lockShard struct {
	mu      *sync.Mutex
	cond    *sync.Cond
	holders map[uint64]TransId
}

func mkLockShard() *lockShard {
	mu := new(sync.Mutex)
	a := &lockShard{
		mu:      mu,
		cond:    sync.NewCond(mu),
		holders: make(map[uint64]TransId),
	}
	return a
}

func (lmap *lockShard) acquire(addr uint64, id TransId) {
	lmap.mu.Lock()
	for {
		_, held := lmap.holders[addr]
		if !held {
			lmap.holders[addr] = id
			break
		}

		lmap.cond.Wait()
	}
	lmap.mu.Unlock()
}

func (lmap *lockShard) release(addr uint64) {
	lmap.mu.Lock()
	delete(lmap.holders, addr)
	lmap.mu.Unlock()
	lmap.cond.Broadcast()
}

const NSHARD uint64 = 43

type lockMap struct {
	shards []*lockShard
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

func (lmap *lockMap) acquire(flataddr uint64, id TransId) {
	shard := lmap.shards[flataddr%NSHARD]
	shard.acquire(flataddr, id)
}

func (lmap *lockMap) release(flataddr uint64, id TransId) {
	shard := lmap.shards[flataddr%NSHARD]
	shard.release(flataddr)
}
