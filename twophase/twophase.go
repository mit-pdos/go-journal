package twophase

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/addr"
	"github.com/mit-pdos/goose-nfsd/buftxn"
	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/lockmap"
	"github.com/mit-pdos/goose-nfsd/txn"
	"github.com/mit-pdos/goose-nfsd/util"
)

type TwoPhasePre struct {
	txn   *txn.Txn
	locks *lockmap.LockMap
}

type TwoPhase struct {
	buftxn   *buftxn.BufTxn
	locks    *lockmap.LockMap
	acquired []uint64
}

func Init(d disk.Disk) *TwoPhasePre {
	twophasePre := &TwoPhasePre{
		txn:   txn.MkTxn(d),
		locks: lockmap.MkLockMap(),
	}
	return twophasePre
}

// Start a local transaction with no writes from a global Txn manager.
func Begin(twophasePre *TwoPhasePre) *TwoPhase {
	trans := &TwoPhase{
		buftxn:   buftxn.Begin(twophasePre.txn),
		locks:    twophasePre.locks,
		acquired: make([]common.Bnum, 0),
	}
	util.DPrintf(5, "tp Begin: %v\n", trans)
	return trans
}

func (twophase *TwoPhase) acquireNoCheck(addr addr.Addr) {
	flatAddr := addr.Flatid()
	twophase.locks.Acquire(flatAddr)
	twophase.acquired = append(twophase.acquired, flatAddr)
}

func (twophase *TwoPhase) isAlreadyAcquired(addr addr.Addr) bool {
	flatAddr := addr.Flatid()
	var already_acquired = false
	for _, acq := range twophase.acquired {
		if flatAddr == acq {
			already_acquired = true
		}
	}
	return already_acquired
}

func (twophase *TwoPhase) Acquire(addr addr.Addr) {
	already_acquired := twophase.isAlreadyAcquired(addr)
	if !already_acquired {
		twophase.acquireNoCheck(addr)
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
	twophase.Acquire(addr)
	return twophase.readBufNoAcquire(addr, sz)
}

// OverWrite writes an object to addr
func (twophase *TwoPhase) OverWrite(addr addr.Addr, sz uint64, data []byte) {
	twophase.Acquire(addr)
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
	util.DPrintf(5, "tp Commit %p\n", twophase)
	return twophase.buftxn.CommitWait(true)
}

func (twophase *TwoPhase) Commit() bool {
	ok := twophase.CommitNoRelease()
	twophase.ReleaseAll()
	return ok
}
