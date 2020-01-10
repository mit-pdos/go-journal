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
	l.logLock.Lock()
	for !l.shutdown {
		blkcount, txn := l.logInstall()
		if blkcount > 0 {
			util.DPrintf(5, "Installed till txn %d\n", txn)
		}
		l.condInstall.Wait()
	}
	l.logLock.Unlock()
}

func (l *Walog) installBlocks(bufs []buf.Buf) {
	n := uint64(len(bufs))
	for i := uint64(0); i < n; i++ {
		blkno := bufs[i].Addr.Blkno
		blk := bufs[i].Blk
		util.DPrintf(5, "installBlocks: write log block %d to %d\n", i, blkno)
		disk.Write(blkno, blk)
	}
}

// Installer holds logLock
// XXX absorp
func (l *Walog) logInstall() (uint64, LogPosition) {
	l.memLock.Lock()
	installEnd := l.diskEnd
	bufs := l.memLog[:installEnd-l.memStart]
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
	l.memLock.Unlock()

	return uint64(len(bufs)), installEnd
}
