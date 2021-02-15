package twophase

import (
	"github.com/mit-pdos/goose-nfsd/addr"
	"github.com/mit-pdos/goose-nfsd/buftxn"
	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/lockmap"
	"github.com/mit-pdos/goose-nfsd/txn"
	"github.com/mit-pdos/goose-nfsd/util"
)

type TwoPhase struct {
	buftxn   *buftxn.BufTxn
	locks    *lockmap.LockMap
	acquired []common.Bnum
}

// Start a local transaction with no writes from a global Txn manager.
func Begin(txn *txn.Txn, l *lockmap.LockMap) *TwoPhase {
	trans := &TwoPhase{
		buftxn:   buftxn.Begin(txn),
		locks:    l,
		acquired: make([]common.Bnum, 0),
	}
	util.DPrintf(1, "tp Begin: %v\n", trans)
	return trans
}

func (twophase *TwoPhase) acquireNoCheck(bnum uint64) {
	twophase.locks.Acquire(bnum)
	twophase.acquired = append(twophase.acquired, bnum)
}

func (twophase *TwoPhase) isAlreadyAcquired(bnum uint64) bool {
	var already_acquired = false
	for _, acq := range twophase.acquired {
		if bnum == acq {
			already_acquired = true
		}
	}
	return already_acquired
}

func (twophase *TwoPhase) Acquire(bnum uint64) {
	already_acquired := twophase.isAlreadyAcquired(bnum)
	if !already_acquired {
		twophase.acquireNoCheck(bnum)
	}
}

func (twophase *TwoPhase) Release() {
	last_index := len(twophase.acquired) - 1
	twophase.locks.Release(twophase.acquired[last_index])
	twophase.acquired = twophase.acquired[:last_index]
}

func (twophase *TwoPhase) ReleaseAll() {
	for len(twophase.acquired) != 0 {
		twophase.Release()
	}
}

func (twophase *TwoPhase) readBufNoAcquire(addr addr.Addr, sz uint64) []byte {
	// PERFORMANCE-IMPACTING HACK:
	// Copying out the data to a new slice isn't necessary,
	// but we need to make it explicit to the proof that we
	// aren't using the read-modify feature of buftxn.
	s := util.CloneByteSlice(twophase.buftxn.ReadBuf(addr, sz).Data)
	return s
}

func (twophase *TwoPhase) ReadBuf(addr addr.Addr, sz uint64) []byte {
	twophase.Acquire(addr.Blkno)
	return twophase.readBufNoAcquire(addr, sz)
}

// OverWrite writes an object to addr
func (twophase *TwoPhase) OverWrite(addr addr.Addr, sz uint64, data []byte) {
	twophase.Acquire(addr.Blkno)
	twophase.buftxn.OverWrite(addr, sz, data)
}

// NDirty reports an upper bound on the size of this transaction when committed.
//
// The caller cannot rely on any particular properties of this function for
// safety.
func (twophase *TwoPhase) NDirty() uint64 {
	return twophase.buftxn.NDirty()
}

// LogSz returns 511
func (twophase *TwoPhase) LogSz() uint64 {
	return twophase.buftxn.LogSz()
}

// LogSzBytes returns 511*4096
func (twophase *TwoPhase) LogSzBytes() uint64 {
	return twophase.buftxn.LogSzBytes()
}

func (twophase *TwoPhase) CommitNoRelease() bool {
	util.DPrintf(1, "tp Commit %p\n", twophase)
	return twophase.buftxn.CommitWait(true)
}

func (twophase *TwoPhase) Commit() bool {
	ok := twophase.CommitNoRelease()
	twophase.ReleaseAll()
	return ok
}
