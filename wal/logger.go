package wal

import (
	"github.com/goose-lang/primitive"
	"github.com/mit-pdos/go-journal/util"
)

// Waits on the installer thread to free space in the log so everything
// logged fits on disk.
//
// establishes uint64(len(l.memLog)) <= LOGSZ
func (l *Walog) waitForSpace() {
	// Wait until there is sufficient space on disk for the entire
	// in-memory log (i.e., the installer must catch up).
	for uint64(len(l.st.memLog.log)) > LOGSZ {
		l.condInstall.Wait()
	}
}

func (l *Walog) flushIfNeeded() {
	if l.st.memLog.needFlush {
		l.st.memLog.clearMutable()
		l.st.memLog.needFlush = false
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
func (l *Walog) logAppend(circ *circularAppender) bool {
	l.waitForSpace()
	l.flushIfNeeded()

	diskEnd := l.st.diskEnd
	newbufs := l.st.memLog.takeFrom(diskEnd)
	if len(newbufs) == 0 {
		return false
	}
	l.memLock.Unlock()

	circ.Append(l.d, diskEnd, newbufs)

	l.memLock.Lock()

	primitive.Linearize()

	l.st.diskEnd = diskEnd + LogPosition(len(newbufs))
	l.condLogger.Broadcast()
	l.condInstall.Broadcast()

	return true
}

// logger writes blocks from the in-memory log to the on-disk log
//
// Operates by continuously polling for in-memory transactions, driven by
// condLogger for scheduling
func (l *Walog) logger(circ *circularAppender) {
	l.memLock.Lock()
	l.st.nthread += 1
	for !l.st.shutdown {
		progress := l.logAppend(circ)
		if !progress {
			l.condLogger.Wait()
		}
	}
	util.DPrintf(1, "logger: shutdown\n")
	l.st.nthread -= 1
	l.condShut.Signal()
	l.memLock.Unlock()
}
