package lockmap

import (
	"sync"
)

type lockState struct {
	owner   uint64 // for debugging
	held    bool
	cond    *sync.Cond
	waiters uint64
}

type lockShard struct {
	mu    *sync.Mutex
	state map[uint64]*lockState
}

func mkLockShard() *lockShard {
	state := make(map[uint64]*lockState)
	mu := new(sync.Mutex)
	a := &lockShard{
		mu:    mu,
		state: state,
	}
	return a
}

func (lmap *lockShard) acquire(addr uint64, id uint64) {
	var done bool
	lmap.mu.Lock()
	for !done {
		var state *lockState
		state1, ok1 := lmap.state[addr]
		if ok1 {
			state = state1
		} else {
			// Allocate a new state
			state = &lockState{
				owner:   id,
				held:    false,
				cond:    sync.NewCond(lmap.mu),
				waiters: 0,
			}
			lmap.state[addr] = state
		}

		if !state.held {
			state.held = true
			state.owner = id
			done = true
		} else {
			state.waiters += 1
			state.cond.Wait()

			state2, ok2 := lmap.state[addr]
			if ok2 {
				// Should always be true, but we don't need to prove this
				state2.waiters -= 1
			}
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

type LockMap struct {
	shards []*lockShard
}

func MkLockMap() *LockMap {
	shards := make([]*lockShard, NSHARD)
	for i := uint64(0); i < NSHARD; i++ {
		shards[i] = mkLockShard()
	}
	a := &LockMap{
		shards: shards,
	}
	return a
}

func (lmap *LockMap) Acquire(flataddr uint64, id uint64) {
	shard := lmap.shards[flataddr%NSHARD]
	shard.acquire(flataddr, id)
}

func (lmap *LockMap) Release(flataddr uint64, id uint64) {
	shard := lmap.shards[flataddr%NSHARD]
	shard.release(flataddr)
}
