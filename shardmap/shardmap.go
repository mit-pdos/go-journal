package shardmap

// TODO wal dependency is only to get Update definition 
import (
	"sort"
	"sync"
	"github.com/tchajed/goose/machine/disk"
	"github.com/mit-pdos/go-journal/common"
)

type mapShard struct {
	mu    *sync.RWMutex
	state map[uint64]disk.Block
}

type BlockMap struct {
	shards []*mapShard
}

const NSHARD uint64 = 65537

func mkMapShard() *mapShard {
	state := make(map[uint64]disk.Block)
	mu := new(sync.RWMutex)
	a := &mapShard{
		mu:    mu,
		state: state,
	}
	return a
}

func MkBlockMap() *BlockMap {
	var shards []*mapShard
	for i := uint64(0); i < NSHARD; i++ {
		shards = append(shards, mkMapShard())
	}
	a := &BlockMap{
		shards: shards,
	}
	return a
}

func (bmap *BlockMap) GetShardNo(addr uint64) uint64 {
	return addr%NSHARD
}

func (bmap *BlockMap) GetShard(addr uint64) *mapShard {
	shard := bmap.shards[bmap.GetShardNo(addr)]
	return shard
}

func (bmap *BlockMap) Read(addr uint64) (disk.Block, bool) {
	shard := bmap.GetShard(addr)
	shard.mu.RLock()
	blk0, ok := shard.state[addr]
	blk = util.CloneByteSlice(blk0)
	shard.mu.RUnlock()
	return blk, ok
}

func (bmap *BlockMap) Write(addr uint64, blk disk.Block) {
	shard := bmap.GetShard(addr)
	shard.mu.Lock()
	shard.state[addr] = blk
	shard.mu.Unlock()
}

func (bmap *BlockMap) MultiWrite(bufs []common.Update) {
	shardnolist := make([]uint64, 0, len(bufs))
	for _, b := range bufs {
		shardnolist = append(shardnolist, bmap.GetShardNo(b.Addr))
	}

	shardnolist_uniq := make([]common.Bnum, 0, len(shardnolist))

	// TODO: Make a helper function for sorting & making uniq
	sort.Slice(shardnolist, func (i, j int) bool { return shardnolist[i] < shardnolist[j] })
	shardnolist_uniq = append(shardnolist_uniq, shardnolist[0])
	var last = shardnolist[0]
	for _, bno := range shardnolist {
		if bno != last {
			shardnolist_uniq = append(shardnolist_uniq, bno)
			last = bno
		}
	}

	// Lock all
	for _, shardno := range shardnolist_uniq {
		shard := bmap.shards[shardno]
		shard.mu.Lock()
	}

	// Write
	for _, buf := range bufs {
		shard := bmap.GetShard(buf.Addr)
		shard.state[buf.Addr] = buf.Block
	}

	// Release all
	for _, shardno := range shardnolist_uniq {
		shard := bmap.shards[shardno]
		shard.mu.Unlock()
	}
}
