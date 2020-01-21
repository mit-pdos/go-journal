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

type TransId uint64

type Txn struct {
	mu     *sync.Mutex
	log    *wal.Walog
	fs     *fs.FsSuper
	locks  *lockMap // map of locks for disk objects
	nextId TransId
}

func MkTxn(fs *fs.FsSuper) *Txn {
	txn := &Txn{
		mu:    new(sync.Mutex),
		log:   wal.MkLog(fs.Disk),
		fs:    fs,
		locks: mkLockMap(),
	}
	return txn
}

// Return a unique Id for a transaction
func (txn *Txn) GetTransId() TransId {
	txn.mu.Lock()
	id := txn.nextId
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

// Lock a disk object
func (txn *Txn) Acquire(addr buf.Addr, id TransId) {
	txn.locks.acquire(addr, id)
}

// Release lock on buf of trans id
func (txn *Txn) Release(addr buf.Addr, id TransId) {
	txn.locks.release(addr, id)
}

// Release all locks used by trans id
func (txn *Txn) releaseTxn(addrs []buf.Addr, id TransId) {
	util.DPrintf(15, "releaseTxn: %v\n", addrs)
	for _, a := range addrs {
		txn.locks.release(a, id)
	}
}

// Last buf that has data for same block
func lastBuf(bufs []*buf.Buf) uint64 {
	i := uint64(0)
	blkno := bufs[i].Addr.Blkno
	l := uint64(len(bufs))
	for ; i < l && blkno == bufs[i].Addr.Blkno; i++ {
	}
	return i
}

// Install bufs that contain data for the same block
func (txn *Txn) installBlock(blk disk.Block, bufs []*buf.Buf) {
	l := len(bufs)
	util.DPrintf(5, "installBlock %v #bufs %d\n", bufs[0].Addr.Blkno, l)
	for i := 0; i < l; i++ {
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
		util.DPrintf(15, "lastbuf %v %d %v\n", bufs[i].Addr, n)
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

// Commit dirty blocks of the transaction into the log, and perhaps
// wait. In either case, release the transaction's locked addresses.
// addrs may include addresses beyond the ones in bufs; for example,
// disk objects that the transaction has read, but not modified.
func (txn *Txn) CommitWait(addrs []buf.Addr, bufs []*buf.Buf, wait bool, abort bool, id TransId) bool {
	n, ok := txn.doCommit(bufs, abort)
	if !ok {
		util.DPrintf(10, "memappend failed; log is too small\n")
	} else {
		if wait {
			txn.log.LogAppendWait(n)
		}
		txn.releaseTxn(addrs, id)
	}
	return ok
}

func (txn *Txn) Flush(addrs []buf.Addr, id TransId) bool {
	txn.log.WaitFlushMemLog()
	txn.releaseTxn(addrs, id)
	return true
}

func (txn *Txn) LogSz() uint64 {
	return txn.log.LogSz()
}

func (txn *Txn) Shutdown() {
	txn.log.Shutdown()
}
