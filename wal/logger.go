package wal

import (
	"github.com/mit-pdos/goose-nfsd/util"
)

//
// Logger writes blocks from the in-memory log to the on-disk log
//

func (l *Walog) logBlocks(memend LogPosition, memstart LogPosition, diskend LogPosition, bufs []BlockData) {
	for pos := diskend; pos < memend; pos++ {
		buf := bufs[pos-diskend]
		blk := buf.blk
		blkno := buf.bn
		util.DPrintf(1, "logBlocks: %d to log block %d\n", blkno, pos)
		l.d.Write(LOGSTART+(uint64(pos)%l.LogSz()), blk)
	}
}

// Logger holds logLock
func (l *Walog) logAppend() bool {
	// Wait until there is sufficient space on disk for the entire
	// in-memory log (i.e., the installer must catch up).
	for {
		if uint64(len(l.memLog)) <= l.LogSz() {
			break
		}

		l.condInstall.Wait()
	}

	memstart := l.memStart
	memlog := l.memLog
	memend := l.commitTxn
	diskend := l.diskEnd
	newbufs := memlog[diskend-memstart : memend-memstart]
	if len(newbufs) == 0 {
		return false
	}

	l.memLock.Unlock()

	l.logBlocks(memend, memstart, diskend, newbufs)

	addrs := make([]uint64, l.LogSz())
	for i := uint64(0); i < uint64(memend-memstart); i++ {
		pos := memstart + LogPosition(i)
		addrs[uint64(pos)%l.LogSz()] = memlog[i].bn
	}
	newh := &hdr{
		end:   memend,
		addrs: addrs,
	}
	l.writeHdr(newh)
	l.d.Barrier()

	l.memLock.Lock()
	l.diskEnd = memend
	l.condLogger.Broadcast()
	l.condInstall.Broadcast()

	return true
}

func (l *Walog) logger() {
	l.memLock.Lock()
	l.nthread++
	for !l.shutdown {
		progress := l.logAppend()
		if !progress {
			l.condLogger.Wait()
		}
	}
	util.DPrintf(1, "logger: shutdown\n")
	l.nthread--
	l.condShut.Signal()
	l.memLock.Unlock()
}
