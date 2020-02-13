package wal

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/util"
)

// logBlocks writes bufs to the end of the circular log
//
// Requires diskEnd to reflect the on-disk log, but otherwise operates without
// holding any locks (with exclusive ownership of the on-disk log).
//
// The caller is responsible for updating both the disk and memory copy of
// diskEnd.
func logBlocks(d disk.Disk, diskEnd LogPosition,
	bufs []Update) {
	for i, buf := range bufs {
		pos := diskEnd + LogPosition(i)
		blk := buf.Block
		blkno := buf.Addr
		util.DPrintf(5,
			"logBlocks: %d to log block %d\n", blkno, pos)
		d.Write(posToDiskAddr(pos), blk)
	}
}

// logAppend appends to the log, if it can find transactions to append.
//
// It grabs the new writes in memory and not on disk through l.nextDiskEnd; if
// there are any such writes, it commits them atomically.
//
// assumes caller holds memLock
//
// Returns true if it made progress (for liveness, not important for
// correctness).
func (l *Walog) logAppend() bool {
	// Wait until there is sufficient space on disk for the entire
	// in-memory log (i.e., the installer must catch up).
	for uint64(len(l.memLog)) > LOGSZ {
		l.condInstall.Wait()
	}
	// establishes uint64(len(l.memLog)) <= LOGSZ

	memstart := l.memStart
	memlog := l.memLog
	newDiskEnd := l.nextDiskEnd
	diskEnd := l.diskEnd
	newbufs := memlog[diskEnd-memstart : newDiskEnd-memstart]
	if len(newbufs) == 0 {
		return false
	}

	l.memLock.Unlock()

	// 1. Update the blocks in the log.
	logBlocks(l.d, diskEnd, newbufs)

	// 2. Extend the addresses on disk with the newbufs addresses.
	addrs := make([]common.Bnum, HDRADDRS)
	// note that this is the old on-disk addresses (through diskEnd-memstart)
	// plus the new ones (through newDiskEnd-memstart)
	for i, buf := range memlog[:newDiskEnd-memstart] {
		pos := memstart + LogPosition(i)
		addrs[uint64(pos)%LOGSZ] = buf.Addr
	}
	newh := &hdr{
		end:   newDiskEnd,
		addrs: addrs,
	}
	// 3. Update the on-disk log to include the new Update.
	l.writeHdr(newh)
	l.d.Barrier()

	l.memLock.Lock()
	l.diskEnd = newDiskEnd
	l.condLogger.Broadcast()
	l.condInstall.Broadcast()

	return true
}

// logger writes blocks from the in-memory log to the on-disk log
//
// Operates by continuously polling for in-memory transactions, driven by
// condLogger for scheduling
func (l *Walog) logger() {
	l.memLock.Lock()
	l.nthread += 1
	for !l.shutdown {
		progress := l.logAppend()
		if !progress {
			l.condLogger.Wait()
		}
	}
	util.DPrintf(1, "logger: shutdown\n")
	l.nthread -= 1
	l.condShut.Signal()
	l.memLock.Unlock()
}
