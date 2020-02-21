package wal

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/util"
)

func (ls *WalogState) cutMemLog(installEnd LogPosition) {
	// delete from memLogMap, if most recent version of blkno
	for i, blk := range ls.memLog[:installEnd-ls.memStart] {
		pos := ls.memStart + LogPosition(i)
		blkno := blk.Addr
		oldPos, ok := ls.memLogMap[blkno]
		if ok && oldPos <= pos {
			util.DPrintf(5, "memLogMap: del %d %d\n", blkno, oldPos)
			delete(ls.memLogMap, blkno)
		}
	}
	// shorten memLog
	ls.memLog = ls.memLog[installEnd-ls.memStart:]
	ls.memStart = installEnd
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
