package wal

import (
	"github.com/mit-pdos/goose-nfsd/util"
)

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
	diskEnd := l.circ.diskEnd
	newbufs := memlog[diskEnd-memstart : newDiskEnd-memstart]
	if len(newbufs) == 0 {
		return false
	}
	l.memLock.Unlock()

	l.circ.Append(newbufs)

	l.memLock.Lock()
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
