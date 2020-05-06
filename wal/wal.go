package wal

import (
	"sync"

	"github.com/tchajed/goose/machine"

	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/util"
)

func mkLog(disk disk.Disk) *Walog {
	circ, start, end, memLog := recoverCircular(disk)
	ml := new(sync.Mutex)
	st := &WalogState{
		memLog:   mkSliding(memLog, start),
		diskEnd:  end,
		shutdown: false,
		nthread:  0,
	}
	l := &Walog{
		d:           disk,
		circ:        circ,
		memLock:     ml,
		st:          st,
		condLogger:  sync.NewCond(ml),
		condInstall: sync.NewCond(ml),
		condShut:    sync.NewCond(ml),
	}
	util.DPrintf(1, "mkLog: size %d\n", LOGSZ)
	return l
}

func (l *Walog) startBackgroundThreads() {
	go func() { l.logger() }()
	go func() { l.installer() }()
}

func MkLog(disk disk.Disk) *Walog {
	l := mkLog(disk)
	l.startBackgroundThreads()
	return l
}

// memWrite writes out bufs to the in-memory log
//
// Absorbs writes in in-memory transactions (avoiding those that might be in
// the process of being logged or installed).
//
// Assumes caller holds memLock
func (st *WalogState) memWrite(bufs []Update) {
	var pos = st.memLog.end()
	for _, buf := range bufs {
		// remember most recent position for Blkno
		oldpos, ok := st.memLog.posForAddr(buf.Addr)
		if ok && oldpos >= st.memLog.mutable {
			util.DPrintf(5, "memWrite: absorb %d pos %d old %d\n",
				buf.Addr, pos, oldpos)
			// the ownership of this part of the memLog is complicated; maybe the
			// logger and installer don't ever take ownership of it, which is why
			// it's safe to write here?
			st.memLog.update(oldpos, buf)
			// note that pos does not need to be incremented
		} else {
			if ok {
				util.DPrintf(5, "memLogMap: replace %d pos %d old %d\n",
					buf.Addr, pos, oldpos)
			} else {
				util.DPrintf(5, "memLogMap: add %d pos %d\n",
					buf.Addr, pos)
			}
			st.memLog.append(buf)
			pos += 1
		}
	}
	// l.condLogger.Broadcast()
}

// Assumes caller holds memLock
func (st *WalogState) doMemAppend(bufs []Update) LogPosition {
	st.memWrite(bufs)
	txn := st.memLog.end()
	return txn
}

// Grab all of the current transactions and record them for the next group commit (when the logger gets around to it).
//
// This is a separate function purely for verification purposes; the code isn't complicated but we have to manipulate
// some ghost state and justify this value of nextDiskEnd.
//
// Assumes caller holds memLock.
func (st *WalogState) endGroupTxn() {
	st.memLog.clearMutable()
}

//
//  For clients of WAL
//

func copyUpdateBlock(u Update) disk.Block {
	return util.CloneByteSlice(u.Block)
}

// readMem implements ReadMem, assuming memLock is held
func (st *WalogState) readMem(blkno common.Bnum) (disk.Block, bool) {
	pos, ok := st.memLog.posForAddr(blkno)
	if ok {
		util.DPrintf(5, "read memLogMap: read %d pos %d\n", blkno, pos)
		u := st.memLog.get(pos)
		blk := copyUpdateBlock(u)
		return blk, true
	}
	return nil, false
}

// Read from only the in-memory cached state (the unstable and logged parts of
// the wal).
func (l *Walog) ReadMem(blkno common.Bnum) (disk.Block, bool) {
	l.memLock.Lock()
	blk, ok := l.st.readMem(blkno)
	machine.Linearize()
	l.memLock.Unlock()
	return blk, ok
}

// Read from only the installed state (a subset of durable state).
func (l *Walog) ReadInstalled(blkno common.Bnum) disk.Block {
	return l.d.Read(blkno)
}

// Read reads from the latest memory state, but does so in a
// difficult-to-linearize way (specifically, it is future-dependent when to
// linearize between the l.memLog.Unlock() and the eventual disk read, due to
// potential concurrent cache or disk writes).
func (l *Walog) Read(blkno common.Bnum) disk.Block {
	blk, ok := l.ReadMem(blkno)
	if ok {
		return blk
	}
	return l.ReadInstalled(blkno)
}

func (st *WalogState) updatesOverflowU64(newUpdates uint64) bool {
	return util.SumOverflows(uint64(st.memEnd()), newUpdates)
}

// TODO: relate this calculation to the circular log free space
func (st *WalogState) memLogHasSpace(newUpdates uint64) bool {
	memSize := uint64(st.memEnd() - st.diskEnd)
	if memSize+newUpdates > LOGSZ {
		return false
	}
	return true
}

// Append to in-memory log.
//
// On success returns the pos for this append.
//
// On failure guaranteed to be idempotent (failure can only occur in principle,
// due overflowing 2^64 writes)
func (l *Walog) MemAppend(bufs []Update) (LogPosition, bool) {
	if uint64(len(bufs)) > LOGSZ {
		return 0, false
	}

	var txn LogPosition = 0
	var ok = true
	l.memLock.Lock()
	st := l.st
	for {
		if st.updatesOverflowU64(uint64(len(bufs))) {
			ok = false
			break
		}
		if st.memLogHasSpace(uint64(len(bufs))) {
			txn = st.doMemAppend(bufs)
			break
		}
		util.DPrintf(5, "memAppend: log is full; try again")
		// commit everything, stable and unstable trans
		st.endGroupTxn()
		l.condLogger.Broadcast()
		l.condLogger.Wait()
		continue
	}
	l.memLock.Unlock()
	return txn, ok
}

// Flush flushes a transaction pos (and all preceding transactions)
//
// The implementation waits until the logger has appended in-memory log up to
// txn to on-disk log.
func (l *Walog) Flush(pos LogPosition) {
	util.DPrintf(1, "Flush: commit till txn %d\n", pos)
	l.memLock.Lock()
	l.condLogger.Broadcast()
	if pos > l.st.memLog.mutable {
		// Get the logger to log everything written so far.
		//
		// This must be a transaction boundary, and this way we actually don't rely on the caller to pass a valid
		// transaction boundary. The proof assumes this anyway for simplicity in the spec.
		l.st.endGroupTxn()
	}
	for !(pos <= l.st.diskEnd) {
		l.condLogger.Wait()
	}
	machine.Linearize()
	// establishes pos <= l.st.diskEnd
	// (pos is now durably on disk)
	l.memLock.Unlock()
}

// Shutdown logger and installer
func (l *Walog) Shutdown() {
	util.DPrintf(1, "shutdown wal\n")
	l.memLock.Lock()
	l.st.shutdown = true
	l.condLogger.Broadcast()
	l.condInstall.Broadcast()
	for l.st.nthread > 0 {
		util.DPrintf(1, "wait for logger/installer")
		l.condShut.Wait()
	}
	l.memLock.Unlock()
	util.DPrintf(1, "wal done\n")
}
