package wal

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/util"
)

//
// Installer blocks from the on-disk log to their home location.
//

func (l *Walog) installer() {
	l.memLock.Lock()
	l.nthread++
	for !l.shutdown {
		l.condInstall.Wait()
		blkcount, txn := l.logInstall()
		if blkcount > 0 {
			util.DPrintf(5, "Installed till txn %d\n", txn)
		}
	}
	util.DPrintf(1, "installer: shutdown\n")
	l.nthread--
	l.condShut.Signal()
	l.memLock.Unlock()
}

func (l *Walog) installBlocks(bufs []buf.Buf) {
	n := uint64(len(bufs))
	for i := uint64(0); i < n; i++ {
		blkno := bufs[i].Addr.Blkno
		blk := bufs[i].Blk
		util.DPrintf(1, "installBlocks: write log block %d to %d\n", i, blkno)
		disk.Write(blkno, blk)
	}
}

// Installer holds logLock
// XXX absorp
func (l *Walog) logInstall() (uint64, LogPosition) {
	installEnd := l.diskEnd
	bufs := l.memLog[:installEnd-l.memStart]
	if len(bufs) == 0 {
		return 0, installEnd
	}

	l.memLock.Unlock()

	util.DPrintf(1, "logInstall up to %d\n", installEnd)
	l.installBlocks(bufs)
	h := &hdr2{
		start: installEnd,
	}
	l.writeHdr2(h)

	l.memLock.Lock()
	if installEnd < l.memStart {
		panic("logInstall")
	}
	l.memLog = l.memLog[installEnd-l.memStart:]
	l.memStart = installEnd
	l.condInstall.Broadcast()

	return uint64(len(bufs)), installEnd
}
