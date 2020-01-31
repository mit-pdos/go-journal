package wal

import (
	"sync"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/fake-bcache/bcache"
	"github.com/mit-pdos/goose-nfsd/util"
	"github.com/tchajed/goose/machine/disk"
)

func (l *Walog) Recover() {
	h := l.readHdr()
	h2 := l.readHdr2()
	l.memStart = h2.start
	l.diskEnd = h.end
	util.DPrintf(1, "Recover %d %d\n", l.memStart, l.diskEnd)
	for pos := h2.start; pos < h.end; pos++ {
		addr := h.addrs[uint64(pos)%l.LogSz()]
		util.DPrintf(1, "recover block %d\n", addr)
		blk := l.d.Read(uint64(LOGSTART) + (uint64(pos) % l.LogSz()))
		b := MkBlockData(addr, blk)
		l.memLog = append(l.memLog, b)
	}
	l.nextDiskEnd = l.memStart + LogPosition(len(l.memLog))
}

func MkLog(disk *bcache.Bcache) *Walog {
	ml := new(sync.Mutex)
	l := &Walog{
		d:           disk,
		memLock:     ml,
		condLogger:  sync.NewCond(ml),
		condInstall: sync.NewCond(ml),
		memLog:      make([]BlockData, 0),
		memStart:    0,
		diskEnd:     0,
		nextDiskEnd: 0,
		shutdown:    false,
		nthread:     0,
		condShut:    sync.NewCond(ml),
		memLogMap:   make(map[buf.Bnum]LogPosition),
	}
	util.DPrintf(1, "mkLog: size %d\n", l.LogSz())

	l.Recover()

	go func() { l.logger() }()
	go func() { l.installer() }()

	return l
}

// Assumes caller holds memLock
func (l *Walog) memWrite(bufs []BlockData) {
	s := LogPosition(len(l.memLog))
	var i uint64 = 0
	for _, buf := range bufs {
		// remember most recent position for Blkno
		pos := l.memStart + s + LogPosition(i)
		oldpos, ok := l.memLogMap[buf.bn]
		if ok && oldpos >= l.nextDiskEnd {
			util.DPrintf(5, "memWrite: absorb %d pos %d old %d\n",
				buf.bn, pos, oldpos)
			l.memLog[oldpos-l.memStart] = buf
		} else {
			if ok {
				util.DPrintf(5, "memLogMap: replace %d pos %d old %d\n",
					buf.bn, pos, oldpos)
			} else {
				util.DPrintf(5, "memLogMap: add %d pos %d\n",
					buf.bn, pos)
			}
			l.memLog = append(l.memLog, buf)
			l.memLogMap[buf.bn] = pos
			i += 1
		}
	}
	// l.condLogger.Broadcast()
}

// Assumes caller holds memLock
func (l *Walog) doMemAppend(bufs []BlockData) LogPosition {
	l.memWrite(bufs)
	txn := l.memStart + LogPosition(len(l.memLog))
	return txn
}

//
//  For clients of WAL
//

// Read blkno from memLog, if present
func (l *Walog) readMemLog(blkno buf.Bnum) disk.Block {
	var blk disk.Block

	l.memLock.Lock()
	pos, ok := l.memLogMap[blkno]
	if ok {
		util.DPrintf(5, "read memLogMap: read %d pos %d\n", blkno, pos)
		buf := l.memLog[pos-l.memStart]
		blk = make([]byte, disk.BlockSize)
		copy(blk, buf.blk)
	}
	l.memLock.Unlock()
	return blk
}

func (l *Walog) Read(blkno buf.Bnum) disk.Block {
	var blk disk.Block

	blkMem := l.readMemLog(blkno)
	if blkMem != nil {
		blk = blkMem
	} else {
		blk = l.d.Read(uint64(blkno))
	}

	return blk
}

// Append to in-memory log. Returns false, if bufs don't fit.
// Otherwise, returns the txn for this append.
func (l *Walog) MemAppend(bufs []BlockData) (LogPosition, bool) {
	if uint64(len(bufs)) > l.LogSz() {
		return 0, false
	}

	var txn LogPosition = 0
	l.memLock.Lock()
	for {
		if uint64(l.memStart)+uint64(len(l.memLog))-uint64(l.diskEnd)+uint64(len(bufs)) > l.LogSz() {
			util.DPrintf(5, "memAppend: log is full; try again")
			// commit everything, stable and unstable trans
			l.nextDiskEnd = l.memStart + LogPosition(len(l.memLog))
			l.condLogger.Broadcast()
			l.condLogger.Wait()
			continue
		}
		txn = l.doMemAppend(bufs)
		break
	}
	l.memLock.Unlock()
	return txn, true
}

// Wait until logger has appended in-memory log up to txn to on-disk
// log
func (l *Walog) LogAppendWait(txn LogPosition) {
	util.DPrintf(1, "LogAppendWait: commit till txn %d\n", txn)
	l.memLock.Lock()
	l.condLogger.Broadcast()
	if txn > l.nextDiskEnd {
		// a concurrent transaction may already committed beyond txn
		l.nextDiskEnd = txn
	}
	for {
		if txn <= l.diskEnd {
			break
		}
		l.condLogger.Wait()
	}
	l.memLock.Unlock()
}

// Wait until last started transaction has been appended to log.  If
// it is logged, then all preceeding transactions are also logged.
func (l *Walog) WaitFlushMemLog() {
	l.memLock.Lock()
	n := l.memStart + LogPosition(len(l.memLog))
	l.memLock.Unlock()

	l.LogAppendWait(n)
}

// Shutdown logger and installer
func (l *Walog) Shutdown() {
	util.DPrintf(1, "shutdown wal\n")
	l.memLock.Lock()
	l.shutdown = true
	l.condLogger.Broadcast()
	l.condInstall.Broadcast()
	for l.nthread > 0 {
		util.DPrintf(1, "wait for logger/installer")
		l.condShut.Wait()
	}
	l.memLock.Unlock()
	util.DPrintf(1, "wal done\n")
}
