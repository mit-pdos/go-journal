package wal

import (
	"github.com/mit-pdos/goose-nfsd/util"
)

func (l *Walog) cutMemLog(installEnd LogPosition) {
	// delete from memLogMap, if most recent version of blkno
	for i := l.memStart; i < installEnd; i++ {
		blkno := l.memLog[i-l.memStart].bn
		pos, ok := l.memLogMap[blkno]
		if ok && pos == i {
			util.DPrintf(5, "memLogMap: del %d %d\n", blkno, pos)
			delete(l.memLogMap, blkno)
		}
	}
	// shorten memLog
	l.memLog = l.memLog[installEnd-l.memStart:]
	l.memStart = installEnd
}

// installBlocks installs the updates in bufs to the data region
//
// Does not hold the memLock, but expects exclusive ownership of the data
// region.
func (l *Walog) installBlocks(bufs []BlockData) {
	for i, buf := range bufs {
		blkno := buf.bn
		blk := buf.blk
		util.DPrintf(5, "installBlocks: write log block %d to %d\n", i, blkno)
		l.d.Write(uint64(blkno), blk)
	}
}

// logInstall installs one on-disk transaction from the disk log to the data
// region.
//
// Returns (blkCount, installEnd)
//
// blkCount is the number of blocks installed (only used for liveness)
//
// installEnd is the new last position installed to the data region (only used
// for debugging)
//
// Installer holds memLock
// XXX absorb
func (l *Walog) logInstall() (uint64, LogPosition) {
	installEnd := l.diskEnd
	bufs := l.memLog[:installEnd-l.memStart]
	if len(bufs) == 0 {
		return 0, installEnd
	}

	l.memLock.Unlock()

	util.DPrintf(5, "logInstall up to %d\n", installEnd)
	l.installBlocks(bufs)
	h := &hdr2{
		start: installEnd,
	}
	l.writeHdr2(h)

	l.memLock.Lock()
	if installEnd < l.memStart {
		panic("logInstall")
	}
	l.cutMemLog(installEnd)
	l.condInstall.Broadcast()

	return uint64(len(bufs)), installEnd
}

// installer installs blocks from the on-disk log to their home location.
func (l *Walog) installer() {
	l.memLock.Lock()
	l.nthread += 1
	for !l.shutdown {
		blkcount, txn := l.logInstall()
		if blkcount > 0 {
			util.DPrintf(5, "Installed till txn %d\n", txn)
		} else {
			l.condInstall.Wait()
		}
	}
	util.DPrintf(1, "installer: shutdown\n")
	l.nthread -= 1
	l.condShut.Signal()
	l.memLock.Unlock()
}
