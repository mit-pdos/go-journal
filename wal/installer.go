package wal

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/util"
)

// cutMemLog deletes from the memLog through installEnd, after these blocks have
// been installed. This transitions from a state where the on-disk install point
// is already at installEnd, but memStart < installEnd.
//
// Assumes caller holds memLock
func (st *WalogState) cutMemLog(installEnd LogPosition) {
	// delete from memLogMap, if most recent version of blkno
	for i, blk := range st.memLog[:installEnd-st.memStart] {
		pos := st.memStart + LogPosition(i)
		blkno := blk.Addr
		oldPos, ok := st.memLogMap[blkno]
		if ok && oldPos <= pos {
			util.DPrintf(5, "memLogMap: del %d %d\n", blkno, oldPos)
			delete(st.memLogMap, blkno)
		}
	}
	// shorten memLog
	st.memLog = st.memLog[installEnd-st.memStart:]
	st.memStart = installEnd
}

// installBlocks installs the updates in bufs to the data region
//
// Does not hold the memLock, but expects exclusive ownership of the data
// region.
func installBlocks(d disk.Disk, bufs []Update) {
	for i, buf := range bufs {
		blkno := buf.Addr
		blk := buf.Block
		util.DPrintf(5, "installBlocks: write log block %d to %d\n", i, blkno)
		d.Write(blkno, blk)
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
func (l *Walog) logInstall() (uint64, LogPosition) {
	installEnd := l.st.diskEnd
	bufs := l.st.memLog[:installEnd-l.st.memStart]
	if len(bufs) == 0 {
		return 0, installEnd
	}

	l.memLock.Unlock()

	util.DPrintf(5, "logInstall up to %d\n", installEnd)
	installBlocks(l.d, bufs)
	Advance(l.d, installEnd)

	l.memLock.Lock()
	l.st.cutMemLog(installEnd)
	l.condInstall.Broadcast()

	return uint64(len(bufs)), installEnd
}

// installer installs blocks from the on-disk log to their home location.
func (l *Walog) installer() {
	l.memLock.Lock()
	l.st.nthread += 1
	for !l.st.shutdown {
		blkcount, txn := l.logInstall()
		if blkcount > 0 {
			util.DPrintf(5, "Installed till txn %d\n", txn)
		} else {
			l.condInstall.Wait()
		}
	}
	util.DPrintf(1, "installer: shutdown\n")
	l.st.nthread -= 1
	l.condShut.Signal()
	l.memLock.Unlock()
}
