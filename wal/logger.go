package wal

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/util"
)

//
// Logger writes blocks from the in-memory log to the on-disk log
//

func (l *Walog) logBlocks(memhead uint64, memtail uint64, diskhead uint64, bufs []buf.Buf) {
	for i := diskhead; i < memhead; i++ {
		bindex := i - diskhead
		blk := bufs[bindex].Blk
		blkno := bufs[bindex].Addr.Blkno
		util.DPrintf(5, "logBlocks: %d to log block %d\n", blkno, i-memtail)
		disk.Write(LOGSTART+(i-memtail), blk)
	}
}

// Logger holds logLock
func (l *Walog) logAppend() {
	hdr := l.readHdr()
	l.memLock.Lock()
	memtail := l.memTail
	memlog := l.memLog
	memhead := memtail + uint64(len(memlog))
	if memtail != hdr.tail || memhead < hdr.head {
		panic("logAppend")
	}

	//util.DPrintf("logAppend memhead %d memtail %d diskhead %d disktail %d\n", memhead, memtail, hdr.head, hdr.tail)
	l.memLock.Unlock()
	newbufs := memlog[hdr.head-memtail:]
	l.logBlocks(memhead, memtail, hdr.head, newbufs)

	// XXX we might be logging a stale memtail here..
	l.writeHdr(memhead, memtail, memlog)

	l.diskHead = TxnNum(memhead)
}

func (l *Walog) logger() {
	l.logLock.Lock()
	for !l.shutdown {
		l.logAppend()
		l.condLogger.Wait()
	}
	l.logLock.Unlock()
}
