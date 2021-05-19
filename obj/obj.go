// Package obj atomically installs objects from  modified buffers in their
// corresponding disk blocks and writes the blocks to the write-ahead log.  The
// upper layers are responsible for locking and lock ordering.
package obj

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/go-journal/addr"
	"github.com/mit-pdos/go-journal/buf"
	"github.com/mit-pdos/go-journal/common"
	"github.com/mit-pdos/go-journal/util"
	"github.com/mit-pdos/go-journal/wal"

	"sync"
)

// Log mediates access to object loading and installation.
//
// There is only one Log object.
type Log struct {
	mu  *sync.Mutex
	log *wal.Walog
	pos wal.LogPosition // highest un-flushed log position
}

// MkLog recovers the object logging system
// (or initializes from an all-zero disk).
func MkLog(d disk.Disk) *Log {
	log := &Log{
		mu:  new(sync.Mutex),
		log: wal.MkLog(d),
		pos: wal.LogPosition(0),
	}
	return log
}

// Read a disk object into buf
func (l *Log) Load(addr addr.Addr, sz uint64) *buf.Buf {
	blk := l.log.Read(addr.Blkno)
	b := buf.MkBufLoad(addr, sz, blk)
	return b
}

// Installs bufs into their blocks and returns the blocks.
// A buf may only partially update a disk block and several bufs may
// apply to the same disk block. Assume caller holds commit lock.
func (l *Log) installBufsMap(bufs []*buf.Buf) map[common.Bnum][]byte {
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
				blk = l.log.Read(b.Addr.Blkno)
				blks[b.Addr.Blkno] = blk
			}
			b.Install(blk)
		}
	}

	return blks
}

func (l *Log) installBufs(bufs []*buf.Buf) []wal.Update {
	var blks []wal.Update
	bufmap := l.installBufsMap(bufs)
	for blkno, data := range bufmap {
		blks = append(blks, wal.MkBlockData(blkno, data))
	}
	return blks
}

// Acquires the commit log, installs the buffers into their
// blocks, and appends the blocks to the in-memory log.
func (l *Log) doCommit(bufs []*buf.Buf) (wal.LogPosition, bool) {
	l.mu.Lock()

	blks := l.installBufs(bufs)

	util.DPrintf(3, "doCommit: %v bufs\n", len(blks))

	n, ok := l.log.MemAppend(blks)
	// FIXME: should only be set if ok
	l.pos = n

	l.mu.Unlock()

	return n, ok
}

// Commit dirty bufs of the transaction into the log, and perhaps wait.
func (l *Log) CommitWait(bufs []*buf.Buf, wait bool) bool {
	var commit = true
	if len(bufs) > 0 {
		n, ok := l.doCommit(bufs)
		if !ok {
			util.DPrintf(10, "memappend failed; log is too small\n")
			commit = false
		} else {
			if wait {
				l.log.Flush(n)
			}
		}
	} else {
		util.DPrintf(5, "commit read-only trans\n")
	}
	return commit
}

// NOTE: this is coarse-grained and unattached to the transaction ID
func (l *Log) Flush() bool {
	l.mu.Lock()
	pos := l.pos
	l.mu.Unlock()

	l.log.Flush(pos)
	return true
}

// LogSz returns 511 (the size of the wal log)
func (l *Log) LogSz() uint64 {
	return wal.LOGSZ
}

func (l *Log) Shutdown() {
	l.log.Shutdown()
}
