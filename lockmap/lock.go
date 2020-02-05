package lockmap

import (
	"sync"
)

type TransId uint64

type lockState struct {
	tid     TransId
	held    bool
	cond    *sync.Cond
	waiters uint64
}

type lockShard struct {
	mu    *sync.Mutex
	state map[uint64]*lockState
}

func mkLockShard() *lockShard {
	mu := new(sync.Mutex)
	a := &lockShard{
		mu:    mu,
		state: make(map[uint64]*lockState),
	}
	return a
}

func (lmap *lockShard) acquire(addr uint64, id TransId) {
	lmap.mu.Lock()
	for {
		state := lmap.state[addr]
		if state == nil {
			// Allocate a new state
			state = &lockState{
				tid:     0,
				held:    false,
				cond:    sync.NewCond(lmap.mu),
				waiters: 0,
			}
			lmap.state[addr] = state
		}

		if !state.held {
			state.held = true
			state.tid = id
			break
		}

		state.waiters += 1
		state.cond.Wait()

		state = lmap.state[addr]
		if state != nil {
			// Should always be true, but we don't need to prove this
			state.waiters -= 1
		}
	}
	lmap.mu.Unlock()
}

func (lmap *lockShard) release(addr uint64) {
	lmap.mu.Lock()
	state := lmap.state[addr]
	state.held = false
	if state.waiters > 0 {
		state.cond.Signal()
	} else {
		delete(lmap.state, addr)
	}
	lmap.mu.Unlock()
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
