package wal

import (
	"sort"

	"github.com/mit-pdos/go-journal/disk"

	"github.com/mit-pdos/go-journal/util"
)

// cutMemLog deletes from the memLog through installEnd, after these blocks have
// been installed. This transitions from a state where the on-disk install point
// is already at installEnd, but memStart < installEnd.
//
// Assumes caller holds memLock
func (st *WalogState) cutMemLog(installEnd LogPosition) {
	st.memLog.deleteFrom(installEnd)
}

// absorbBufs returns bufs' such that applyUpds(d, bufs') = applyUpds(d,
// bufs) and bufs' has unique addresses
func absorbBufs(bufs []Update) []Update {
	s := mkSliding(nil, 0)
	s.memWrite(bufs)
	return s.intoMutable()
}

func batchBlockSplit(bufs2 []Update) [][]Update {
	bufs := append([]Update{}, bufs2...)
	sort.SliceStable(bufs, func(i, j int) bool {
		return bufs[i].Addr < bufs[j].Addr
	})

	var tmpBlks []Update
	var result [][]Update
	for i, buf := range bufs {
		isEnd := i == len(bufs)-1
		if i == 0 {
			tmpBlks = []Update{buf}
		} else if bufs[i-1].Addr == buf.Addr {
			tmpBlks[len(tmpBlks)-1] = buf
		} else {
			isConsecutive := bufs[i-1].Addr+1 == buf.Addr
			if isConsecutive {
				tmpBlks = append(tmpBlks, buf)
			} else {
				result = append(result, tmpBlks)
				tmpBlks = []Update{buf}
			}
		}
		if isEnd {
			result = append(result, tmpBlks)
		}
	}
	return result
}
func installBatchBlocks(db disk.DiskWriteBatch, bufsOrig []Update) {
	bufs := append([]Update{}, bufsOrig...)
	sort.SliceStable(bufs, func(i, j int) bool {
		return bufs[i].Addr < bufs[j].Addr
	})

	splitUpdate := batchBlockSplit(bufs)
	for _, buf := range splitUpdate {
		var blks []disk.Block
		for _, blk := range buf {
			blks = append(blks, blk.Block)
		}
		util.DPrintf(5, "installBlocksBatch: write log block %d to %d\n", buf[0], buf[0].Addr+uint64(len(blks)))
		db.WriteBatch(buf[0].Addr, blks)
	}
}

// installBlocks installs the updates in bufs to the data region
//
// Does not hold the memLock. De-duplicates writes in bufs such that:
// (1) after installBlocks,
// the equivalent of applying bufs in order is accomplished
// (2) at all intermediate points,
// the data region either has the value from the old transaction or the new
// transaction (with all of bufs applied).
func installBlocks(d disk.Disk, bufs []Update) {
	if db, ok := d.(disk.DiskWriteBatch); ok {
		installBatchBlocks(db, bufs)
		return
	}

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
	bufs := l.st.memLog.takeTill(installEnd)
	numBufs := uint64(len(bufs))
	if numBufs == 0 {
		return 0, installEnd
	}

	l.memLock.Unlock()

	util.DPrintf(5, "logInstall up to %d\n", installEnd)
	installBlocks(l.d, bufs)
	l.d.Barrier()
	Advance(l.d, installEnd)

	l.memLock.Lock()
	l.st.cutMemLog(installEnd)
	l.condInstall.Broadcast()

	return numBufs, installEnd
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
