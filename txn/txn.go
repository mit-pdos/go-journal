package txn

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/fs"
	"github.com/mit-pdos/goose-nfsd/util"
	"github.com/mit-pdos/goose-nfsd/wal"

	"sort"
	"sync"
)

//
// txn atomically installs modified buffers in their corresponding
// disk blocks and writes the blocks to the write-ahead log.  The
// upper layers are responsible for locking and lock ordering.
//

type TransId uint64

type Txn struct {
	mu     *sync.Mutex
	log    *wal.Walog
	fs     *fs.FsSuper
	nextId TransId
}

func MkTxn(fs *fs.FsSuper) *Txn {
	txn := &Txn{
		mu:     new(sync.Mutex),
		log:    wal.MkLog(fs.Disk),
		fs:     fs,
		nextId: TransId(0),
	}
	return txn
}

// Return a unique Id for a transaction
func (txn *Txn) GetTransId() TransId {
	txn.mu.Lock()
	var id = txn.nextId
	if id == 0 { // skip 0
		txn.nextId += 1
		id = 1
	}
	txn.nextId += 1
	txn.mu.Unlock()
	return id
}

// Read a disk object into buf
func (txn *Txn) Load(addr buf.Addr) *buf.Buf {
	blk := txn.log.Read(addr.Blkno)
	b := buf.MkBufLoad(addr, blk)
	return b
}

// Last buf in bufs that has data for the same block as the first buf
func lastBuf(bufs []*buf.Buf) uint64 {
	var i = uint64(0)
	blkno := bufs[i].Addr.Blkno
	l := uint64(len(bufs))
	for ; i < l && blkno == bufs[i].Addr.Blkno; i++ {
	}
	return i
}

// Install bufs that contain data for the same block
func (txn *Txn) installBlock(blk disk.Block, bufs []*buf.Buf) {
	l := uint64(len(bufs))
	util.DPrintf(5, "installBlock %v #bufs %d\n", bufs[0].Addr.Blkno, l)
	for i := uint64(0); i < l; i++ {
		bufs[i].Install(blk)
	}
}

// Installs the txn's bufs into their blocks and returns the blocks.
// A buf may only partially update a disk block and several bufs may
// apply to the same disk block. Assume caller holds commit lock.
func (txn *Txn) installBufs(bufs []*buf.Buf) []wal.BlockData {
	var blks = make([]wal.BlockData, 0)
	sort.Slice(bufs, func(i, j int) bool {
		return bufs[i].Addr.Blkno < bufs[j].Addr.Blkno
	})
	l := uint64(len(bufs))
	for i := uint64(0); i < l; {
		n := lastBuf(bufs[i:])
		util.DPrintf(15, "lastbuf %v %d\n", bufs[i].Addr, n)
		var blk []byte
		blkno := bufs[i].Addr.Blkno
		if txn.fs.DiskBlockSize(bufs[i].Addr) {
			// overwrite complete block
			blk = bufs[i].Blk
		} else {
			// read block blkno and install
			blk = txn.log.Read(blkno)
			txn.installBlock(blk, bufs[i:i+n])
		}
		b := wal.MkBlockData(blkno, blk)
		blks = append(blks, b)
		i = i + n
	}
	return blks
}

// Acquires the commit log, installs the txn's buffers into their
// blocks, and appends the blocks to the in-memory log.
func (txn *Txn) doCommit(bufs []*buf.Buf, abort bool) (wal.LogPosition, bool) {
	txn.mu.Lock()

	blks := txn.installBufs(bufs)

	util.DPrintf(3, "doCommit: %v bufs\n", len(blks))

	n, ok := txn.log.MemAppend(blks)

	txn.mu.Unlock()

	return n, ok
}

// Commit dirty bufs of the transaction into the log, and perhaps wait.
func (txn *Txn) CommitWait(bufs []*buf.Buf, wait bool, abort bool, id TransId) bool {
	var commit = true
	if len(bufs) > 0 {
		n, ok := txn.doCommit(bufs, abort)
		if !ok {
			util.DPrintf(10, "memappend failed; log is too small\n")
			commit = false
		} else {
			if wait {
				txn.log.LogAppendWait(n)
			}
		}
	} else {
		util.DPrintf(5, "commit read-only trans\n")
	}
	return commit
}

func (txn *Txn) Flush() bool {
	txn.log.WaitFlushMemLog()
	return true
}

func (txn *Txn) LogSz() uint64 {
	return txn.log.LogSz()
}

func (txn *Txn) Shutdown() {
	txn.log.Shutdown()
}
