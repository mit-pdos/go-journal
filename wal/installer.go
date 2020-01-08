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
func (l *Walog) logInstall() ([]uint64, TxnNum) {
	hdr := l.readHdr()
	bufs := l.memLog[l.index(hdr.tail):l.index(hdr.head)]
	util.DPrintf(1, "logInstall diskhead %d disktail %d\n", hdr.head, hdr.tail)
	l.installBlocks(bufs)
	hdr.tail = hdr.head
	l.writeHdr(hdr.head, hdr.tail, hdr.logTxnNxt, nil)
	l.memLock.Lock()

	if hdr.tail < l.memTail {
		panic("logInstall")
	}
	l.memLog = l.memLog[l.index(hdr.tail):l.index(l.memHead)]
	l.memTail = hdr.tail
	l.dsktxnNxt = hdr.logTxnNxt
	l.memLock.Unlock()
	return hdr.addrs, hdr.logTxnNxt
}
