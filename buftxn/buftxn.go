// buftxn manages "buffer"-based transactions
//
// The caller uses this interface by creating a BufTxn, reading/writing within
// the transaction, and finally committing the buffered transaction.
//
// Note that while the API has reads and writes, these are not the usual database
// read/write transactions. Only writes are made atomic and visible atomically;
// reads are cached on first read. Thus to use this library the file
// system in practice locks (sub-block) objects before running a transaction.
// This is necessary so that loaded objects are read from a consistent view.
//
// Transactions support asynchronous durability by setting wait=false in
// CommitWait. An asynchronous transaction is made visible atomically to other
// threads, including across crashes, but if the system crashes a committed
// asynchronous transaction can be lost. To guarantee that a particular
// transaction is durable, call (*Buftxn) Flush (which flushes all transactions).
//
// Objects have sizes. Implicit in the code is that there is a static "schema"
// that determines the disk layout: each block has objects of a particular size,
// and all sizes used fit an integer number of objects in a block. This schema
// guarantees that objects never overlap, as long as operations involving an
// addr.Addr use the correct size for that block number.
//
// The file system realizes this schema fairly simply, since the disk is simply
// partitioned into inodes, data blocks, and bitmap allocators for each (sized
// appropriately), all allocated statically.
package buftxn

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/addr"
	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/txn"
	"github.com/mit-pdos/goose-nfsd/util"
)

type BufTxn struct {
	txn  *txn.Txn
	bufs *buf.BufMap // map of bufs read/written by this transaction
}

// Start a local transaction with no writes from a global Txn manager.
func Begin(txn *txn.Txn) *BufTxn {
	trans := &BufTxn{
		txn:  txn,
		bufs: buf.MkBufMap(),
	}
	util.DPrintf(1, "Begin: %v\n", trans)
	return trans
}

func (buftxn *BufTxn) ReadBuf(addr addr.Addr, sz uint64) *buf.Buf {
	b := buftxn.bufs.Lookup(addr)
	if b == nil {
		buf := buftxn.txn.Load(addr, sz)
		buftxn.bufs.Insert(buf)
		return buftxn.bufs.Lookup(addr)
	}
	return b
}

// OverWrite writes an object to addr
func (buftxn *BufTxn) OverWrite(addr addr.Addr, sz uint64, data []byte) {
	var b = buftxn.bufs.Lookup(addr)
	if b == nil {
		b = buf.MkBuf(addr, sz, data)
		b.SetDirty()
		buftxn.bufs.Insert(b)
	} else {
		if sz != b.Sz {
			panic("overwrite")
		}
		b.Data = data
		b.SetDirty()
	}
}

// NDirty reports an upper bound on the size of this transaction when committed.
//
// The caller cannot rely on any particular properties of this function for
// safety.
func (buftxn *BufTxn) NDirty() uint64 {
	return buftxn.bufs.Ndirty()
}

// LogSz returns 511
func (buftxn *BufTxn) LogSz() uint64 {
	return buftxn.txn.LogSz()
}

// LogSzBytes returns 511*4096
func (buftxn *BufTxn) LogSzBytes() uint64 {
	return buftxn.txn.LogSz() * disk.BlockSize
}

// CommitWait commits the writes in the transaction to disk.
//
// If CommitWait returns false, the transaction failed and had no logical effect.
// This can happen, for example, if the transaction is too big to fit in the
// on-disk journal.
//
// wait=true is a synchronous commit, which is durable as soon as CommitWait
// returns.
//
// wait=false is an asynchronous commit, which can be made durable later with
// Flush.
func (buftxn *BufTxn) CommitWait(wait bool) bool {
	util.DPrintf(1, "Commit %p w %v\n", buftxn, wait)
	ok := buftxn.txn.CommitWait(buftxn.bufs.DirtyBufs(), wait)
	return ok
}

func (buftxn *BufTxn) Flush() bool {
	ok := buftxn.txn.Flush()
	return ok
}
