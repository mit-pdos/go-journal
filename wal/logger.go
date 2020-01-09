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
	h := l.readHdr()
	l.memLock.Lock()
	memstart := l.memStart
	memlog := l.memLog
	memend := memstart + LogPosition(len(memlog))
	if memstart != h.start || memend < h.end {
		panic("logAppend")
	}

	//util.DPrintf("logAppend memend %d memstart %d diskend %d diskstart %d\n", memend, memstart, h.end, h.start)
	l.memLock.Unlock()
	newbufs := memlog[h.end-memstart:]
	l.logBlocks(memend, memstart, h.end, newbufs)

	// XXX we might be logging a stale memstart here..
	addrs := make([]uint64, len(memlog))
	for i := uint64(0); i < uint64(len(memlog)); i++ {
		addrs[i] = memlog[i].Addr.Blkno
	}
	newh := &hdr{
		end: memend,
		start: memstart,
		addrs: addrs,
	}
	l.writeHdr(newh)

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
