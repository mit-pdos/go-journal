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
		blknos, txn := l.logInstall()
		if len(blknos) > 0 {
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
func (l *Walog) logInstall() ([]uint64, LogPosition) {
	hdr := l.readHdr()
	l.memLock.Lock()
	bufs := l.memLog[hdr.start-l.memStart : hdr.end-l.memStart]
	l.memLock.Unlock()
	util.DPrintf(1, "logInstall diskend %d diskstart %d\n", hdr.end, hdr.start)
	l.installBlocks(bufs)
	hdr.start = hdr.end
	l.writeHdr(hdr.end, hdr.start, nil)
	l.memLock.Lock()

	if hdr.start < l.memStart {
		panic("logInstall")
	}
	l.memLog = l.memLog[hdr.start-l.memStart:]
	l.memStart = hdr.start
	l.memLock.Unlock()
	return hdr.addrs, hdr.end
}
