// lockmap is a sharded lock map.
//
// The API is as if LockMap consisted of a lock for every possible uint64
// (which we think of as "addresses", but they could be any abstract location);
// LockMap.Acquire(a) acquires the lock associated with a and
// LockMap.Release(a) release it.
//
// The implementation doesn't actually maintain all of these locks; it
// instead maintains a fixed collection of shards so that shard i is
// responsible for maintaining the lock state of all a such that a % NSHARDS = i.
// Acquiring a lock requires synchronizing with any threads accessing the same
// shard.
package lockmap

import (
	"sync"
)

type lockState struct {
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

func (lmap *lockShard) acquire(addr uint64) {
	lmap.mu.Lock()
	for {
		var state *lockState
		state1, ok1 := lmap.state[addr]
		if ok1 {
			state = state1
		} else {
			// Allocate a new state
			state = &lockState{
				held:    false,
				cond:    sync.NewCond(lmap.mu),
				waiters: 0,
			}
			lmap.state[addr] = state
		}

		var acquired bool

		if !state.held {
			state.held = true
			acquired = true
		} else {
			state.waiters += 1
			state.cond.Wait()

			state2, ok2 := lmap.state[addr]
			if ok2 {
				// Should always be true, but we don't need to prove this
				state2.waiters -= 1
			}
		}

		if acquired {
			break
		}
		continue
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
	var shards []*lockShard
	for i := uint64(0); i < NSHARD; i++ {
		shards = append(shards, mkLockShard())
	}
	a := &LockMap{
		shards: shards,
	}
	return a
}

func (lmap *LockMap) Acquire(flataddr uint64) {
	shard := lmap.shards[flataddr%NSHARD]
	shard.acquire(flataddr)
}

func (lmap *LockMap) Release(flataddr uint64) {
	shard := lmap.shards[flataddr%NSHARD]
	shard.release(flataddr)
}
