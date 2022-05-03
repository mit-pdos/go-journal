// Package txn implements a full transaction interface on top of GoJournal.
//
// Transactions in this package do not have to implement concurrency control,
// since the package uses two-phase locking to automatically synchronize
// transactions. Lock ordering is still up to the caller to avoid deadlocks.
package txn

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/go-journal/addr"
	"github.com/mit-pdos/go-journal/jrnl"
	"github.com/mit-pdos/go-journal/lockmap"
	"github.com/mit-pdos/go-journal/obj"
	"github.com/mit-pdos/go-journal/util"
)

type Log struct {
	log   *obj.Log
	locks *lockmap.LockMap
}

type Txn struct {
	buftxn   *jrnl.Op
	locks    *lockmap.LockMap
	acquired map[uint64]bool
}

func Init(d disk.Disk) *Log {
	twophasePre := &Log{
		log:   obj.MkLog(d),
		locks: lockmap.MkLockMap(),
	}
	return twophasePre
}

// Start a local transaction with no writes from a global Log.
func Begin(tsys *Log) *Txn {
	trans := &Txn{
		buftxn:   jrnl.Begin(tsys.log),
		locks:    tsys.locks,
		acquired: make(map[uint64]bool),
	}
	util.DPrintf(5, "tp Begin: %v\n", trans)
	return trans
}

func (txn *Txn) acquireNoCheck(addr addr.Addr) {
	flatAddr := addr.Flatid()
	txn.locks.Acquire(flatAddr)
	txn.acquired[flatAddr] = true
}

func (txn *Txn) isAlreadyAcquired(addr addr.Addr) bool {
	flatAddr := addr.Flatid()
	return txn.acquired[flatAddr]
}

func (txn *Txn) Acquire(addr addr.Addr) {
	already_acquired := txn.isAlreadyAcquired(addr)
	if !already_acquired {
		txn.acquireNoCheck(addr)
	}
}

func (txn *Txn) ReleaseAll() {
	for flatAddr := range txn.acquired {
		txn.locks.Release(flatAddr)
	}
}

func (txn *Txn) readBufNoAcquire(addr addr.Addr, sz uint64) []byte {
	// PERFORMANCE-IMPACTING HACK:
	// Copying out the data to a new slice isn't necessary,
	// but we need to make it explicit to the proof that we
	// aren't using the read-modify feature of buftxn.
	s := util.CloneByteSlice(txn.buftxn.ReadBuf(addr, sz).Data)
	return s
}

func (txn *Txn) ReadBuf(addr addr.Addr, sz uint64) []byte {
	txn.Acquire(addr)
	return txn.readBufNoAcquire(addr, sz)
}

// OverWrite writes an object to addr
func (txn *Txn) OverWrite(addr addr.Addr, sz uint64, data []byte) {
	txn.Acquire(addr)
	txn.buftxn.OverWrite(addr, sz, data)
}

func (txn *Txn) ReadBufBit(addr addr.Addr) bool {
	dataByte := txn.ReadBuf(addr, 1)[0]
	return 1 == ((dataByte >> (addr.Off % 8)) & 1)
}

func bitToByte(off uint64, data bool) byte {
	if data {
		return 1 << off
	} else {
		return 0
	}
}

func (txn *Txn) OverWriteBit(addr addr.Addr, data bool) {
	dataBytes := make([]byte, 1)
	dataBytes[0] = bitToByte(addr.Off%8, data)
	txn.OverWrite(addr, 1, dataBytes)
}

// NDirty reports an upper bound on the size of this transaction when committed.
//
// TODO: number of locks acquired also bounds size of transaction
//
// The caller cannot rely on any particular properties of this function for
// safety.
func (txn *Txn) NDirty() uint64 {
	return txn.buftxn.NDirty()
}

func (txn *Txn) commitNoRelease(wait bool) bool {
	util.DPrintf(5, "tp Commit %p\n", txn)
	return txn.buftxn.CommitWait(wait)
}

func (txn *Txn) Commit(wait bool) bool {
	ok := txn.commitNoRelease(wait)
	txn.ReleaseAll()
	return ok
}
