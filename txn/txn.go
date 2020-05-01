package txn

import (
	"github.com/mit-pdos/goose-nfsd/addr"
	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/super"
	"github.com/mit-pdos/goose-nfsd/util"
	"github.com/mit-pdos/goose-nfsd/wal"

	"sync"
)

//
// txn atomically installs modified buffers in their corresponding
// disk blocks and writes the blocks to the write-ahead log.  The
// upper layers are responsible for locking and lock ordering.
//

type TransId = uint64

type Txn struct {
	mu     *sync.Mutex
	log    *wal.Walog
	fs     *super.FsSuper
	nextId TransId
	pos    wal.LogPosition // highest un-flushed log position
}

func MkTxn(fs *super.FsSuper) *Txn {
	txn := &Txn{
		mu:     new(sync.Mutex),
		log:    wal.MkLog(fs.Disk),
		fs:     fs,
		nextId: TransId(0),
		pos:    wal.LogPosition(0),
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
func (txn *Txn) Load(addr addr.Addr, sz uint64) *buf.Buf {
	blk := txn.log.Read(addr.Blkno)
	b := buf.MkBufLoad(addr, sz, blk)
	return b
}

// Installs the txn's bufs into their blocks and returns the blocks.
// A buf may only partially update a disk block and several bufs may
// apply to the same disk block. Assume caller holds commit lock.
func (txn *Txn) installBufsMap(bufs []*buf.Buf) map[common.Bnum][]byte {
	blks := make(map[common.Bnum][]byte)

	for _, b := range bufs {
		if b.Sz == common.NBITBLOCK {
			blks[b.Addr.Blkno] = b.Data
		} else {
			var blk []byte
			mapblk, ok := blks[b.Addr.Blkno]
			if ok {
				blk = mapblk
			} else {
				blk = txn.log.Read(b.Addr.Blkno)
				blks[b.Addr.Blkno] = blk
			}
			b.Install(blk)
		}
	}

	return blks
}

func (txn *Txn) installBufs(bufs []*buf.Buf) []wal.Update {
	var blks []wal.Update
	bufmap := txn.installBufsMap(bufs)
	for blkno, data := range bufmap {
		blks = append(blks, wal.MkBlockData(blkno, data))
	}
	return blks
}

// Acquires the commit log, installs the txn's buffers into their
// blocks, and appends the blocks to the in-memory log.
func (txn *Txn) doCommit(bufs []*buf.Buf) (wal.LogPosition, bool) {
	txn.mu.Lock()

	blks := txn.installBufs(bufs)

	util.DPrintf(3, "doCommit: %v bufs\n", len(blks))

	n, ok := txn.log.MemAppend(blks)
	txn.pos = n

	txn.mu.Unlock()

	return n, ok
}

// Commit dirty bufs of the transaction into the log, and perhaps wait.
func (txn *Txn) CommitWait(bufs []*buf.Buf, wait bool, id TransId) bool {
	var commit = true
	if len(bufs) > 0 {
		n, ok := txn.doCommit(bufs)
		if !ok {
			util.DPrintf(10, "memappend failed; log is too small\n")
			commit = false
		} else {
			if wait {
				txn.log.Flush(n)
			}
		}
	} else {
		util.DPrintf(5, "commit read-only trans\n")
	}
	return commit
}

// NOTE: this is coarse-grained and unattached to the transaction ID
func (txn *Txn) Flush() bool {
	txn.mu.Lock()
	pos := txn.pos
	txn.mu.Unlock()

	txn.log.Flush(pos)
	return true
}

func (txn *Txn) LogSz() uint64 {
	return wal.LOGSZ
}

func (txn *Txn) Shutdown() {
	txn.log.Shutdown()
}
