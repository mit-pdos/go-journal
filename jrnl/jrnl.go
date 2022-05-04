// Package jrnl is the top-level journal API.
//
// It provides atomic operations that are buffered locally and manipulate
// objects via buffers of type *buf.Buf.
//
// The caller uses this interface by beginning an operation Op,
// reading/writing within the transaction, and finally committing the buffered
// transaction.
//
// Note that while the API has reads and writes, these are not the usual database
// read/write transactions. Only writes are made atomic and visible atomically;
// reads are cached on first read. Thus to use this library the file
// system in practice locks (sub-block) objects before using them in an
// operation. This is necessary so that loaded objects are read from a consistent
// view.
//
// Operations support asynchronous durability by setting wait=false in
// CommitWait, which results in an "unstable" operation. An unstable operation is
// made visible atomically to other threads, including across crashes, but if the
// system crashes the latest unstable operations can be lost. To guarantee that a
// particular operation is durable, call Flush on the underlying *obj.Log (which
// flushes all transactions).
//
// Objects have sizes. Implicit in the code is that there is a static "schema"
// that determines the disk layout: each block has objects of a particular size,
// and all sizes used fit an integer number of objects in a block. This schema
// guarantees that objects never overlap, as long as operations involving an
// addr.Addr use the correct size for that block number.
//
// A file system can realize this schema fairly simply, since the disk is
// partitioned into inodes, data blocks, and bitmap allocators for each (sized
// appropriately), all allocated statically.
package jrnl

import (
	"github.com/mit-pdos/go-journal/addr"
	"github.com/mit-pdos/go-journal/buf"
	"github.com/mit-pdos/go-journal/obj"
	"github.com/mit-pdos/go-journal/util"
)

// LogBlocks is the maximum number of blocks that can be written in one
// operation
const LogBlocks uint64 = 511

// LogBytes is the maximum size of an operation, in bytes
const LogBytes uint64 = 4096 * 511

// Op is an in-progress journal operation.
//
// Call CommitWait to persist the operation's writes.
// To abort the operation simply stop using it.
type Op struct {
	log  *obj.Log
	bufs *buf.BufMap // map of bufs read/written by this operation
}

// Begin starts a local journal operation with no writes from a global object
// manager.
func Begin(log *obj.Log) *Op {
	trans := &Op{
		log:  log,
		bufs: buf.MkBufMap(),
	}
	util.DPrintf(3, "Begin: %v\n", trans)
	return trans
}

func (op *Op) ReadBuf(addr addr.Addr, sz uint64) *buf.Buf {
	b := op.bufs.Lookup(addr)
	if b == nil {
		buf := op.log.Load(addr, sz)
		op.bufs.Insert(buf)
		return op.bufs.Lookup(addr)
	}
	return b
}

// OverWrite writes an object to addr
func (op *Op) OverWrite(addr addr.Addr, sz uint64, data []byte) {
	var b = op.bufs.Lookup(addr)
	if b == nil {
		b = buf.MkBuf(addr, sz, data)
		b.SetDirty()
		op.bufs.Insert(b)
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
func (op *Op) NDirty() uint64 {
	return op.bufs.Ndirty()
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
func (op *Op) CommitWait(wait bool) bool {
	util.DPrintf(3, "Commit %p w %v\n", op, wait)
	ok := op.log.CommitWait(op.bufs.DirtyBufs(), wait)
	return ok
}

func (op *Op) Flush() {
	op.log.Flush()
}
