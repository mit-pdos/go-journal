package wal

import (
	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/util"
)

func (l *Walog) logBlocks(memend LogPosition, memstart LogPosition,
	diskend LogPosition, bufs []BlockData) {
	for pos := diskend; pos < memend; pos++ {
		buf := bufs[pos-diskend]
		blk := buf.blk
		blkno := buf.bn
		util.DPrintf(5,
			"logBlocks: %d to log block %d\n", blkno, pos)
		l.d.Write(posToDiskAddr(pos), blk)
	}
}

// logAppend waits for disk log space and then appends to the log
//
// assumes caller holds memLock
func (l *Walog) logAppend() bool {
	// Wait until there is sufficient space on disk for the entire
	// in-memory log (i.e., the installer must catch up).
	for uint64(len(l.memLog)) > LOGSZ {
		l.condInstall.Wait()
	}
	// establishes uint64(len(l.memLog)) <= LOGSZ

	memstart := l.memStart
	memlog := l.memLog
	memend := l.nextDiskEnd
	diskend := l.diskEnd
	newbufs := memlog[diskend-memstart : memend-memstart]
	if len(newbufs) == 0 {
		return false
	}

	l.memLock.Unlock()

	l.logBlocks(memend, memstart, diskend, newbufs)

	addrs := make([]buf.Bnum, HDRADDRS)
	for i := uint64(0); i < uint64(memend-memstart); i++ {
		pos := memstart + LogPosition(i)
		addrs[uint64(pos)%LOGSZ] = memlog[i].bn
	}
	newh := &hdr{
		end:   memend,
		addrs: addrs,
	}
	l.writeHdr(newh)
	l.d.Barrier()

	l.memLock.Lock()
	l.diskEnd = memend
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
