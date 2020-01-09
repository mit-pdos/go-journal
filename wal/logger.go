package wal

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/util"
)

//
// Logger writes blocks from the in-memory log to the on-disk log
//

func (l *Walog) logBlocks(memend LogPosition, memstart LogPosition, diskend LogPosition, bufs []buf.Buf) {
	for i := diskend; i < memend; i++ {
		bindex := i - diskend
		blk := bufs[bindex].Blk
		blkno := bufs[bindex].Addr.Blkno
		util.DPrintf(5, "logBlocks: %d to log block %d\n", blkno, i-memstart)
		disk.Write(LOGSTART+uint64(i-memstart), blk)
	}
}

// Logger holds logLock
func (l *Walog) logAppend() {
	hdr := l.readHdr()
	l.memLock.Lock()
	memstart := l.memStart
	memlog := l.memLog
	memend := memstart + LogPosition(len(memlog))
	if memstart != hdr.start || memend < hdr.end {
		panic("logAppend")
	}

	//util.DPrintf("logAppend memend %d memstart %d diskend %d diskstart %d\n", memend, memstart, hdr.end, hdr.start)
	l.memLock.Unlock()
	newbufs := memlog[hdr.end-memstart:]
	l.logBlocks(memend, memstart, hdr.end, newbufs)

	// XXX we might be logging a stale memstart here..
	l.writeHdr(memend, memstart, memlog)

	l.diskEnd = memend
}

func (l *Walog) logger() {
	l.logLock.Lock()
	for !l.shutdown {
		l.logAppend()
		l.condLogger.Wait()
	}
	l.logLock.Unlock()
}
