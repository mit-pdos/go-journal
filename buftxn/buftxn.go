package buftxn

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/txn"
	"github.com/mit-pdos/goose-nfsd/util"
)

//
// Txn layer used by file system.  A transaction has locked bitmap
// addresses and buffers that it has read/written.
//

type BufTxn struct {
	txn  *txn.Txn
	bufs *buf.BufMap // map of bufs read/written by this transaction
	Id   txn.TransId
}

func Begin(txn *txn.Txn) *BufTxn {
	trans := &BufTxn{
		txn:  txn,
		bufs: buf.MkBufMap(),
		Id:   txn.GetTransId(),
	}
	util.DPrintf(1, "Begin: %v\n", trans.Id)
	return trans
}

// Used for inodes and data blocks
func (buftxn *BufTxn) ReadBuf(addr buf.Addr) *buf.Buf {
	b := buftxn.bufs.Lookup(addr)
	if b == nil {
		buf := buftxn.txn.Load(addr)
		buftxn.bufs.Insert(buf)
	}
	b = buftxn.bufs.Lookup(addr)
	return b
}

// Caller overwrites addr without reading it
func (buftxn *BufTxn) OverWrite(addr buf.Addr, data []byte) {
	b := buftxn.bufs.Lookup(addr)
	if b == nil {
		b = buf.MkBuf(addr, data)
		buftxn.bufs.Insert(b)
	} else {
		if uint64(len(data)*8) != b.Addr.Sz {
			panic("overwrite")
		}
		b.Blk = data
	}
	b.SetDirty()
}

func (buftxn *BufTxn) NDirty() uint64 {
	return buftxn.bufs.Ndirty()
}

func (buftxn *BufTxn) LogSz() uint64 {
	return buftxn.txn.LogSz()
}

func (buftxn *BufTxn) LogSzBytes() uint64 {
	return buftxn.txn.LogSz() * disk.BlockSize
}

// Commit dirty bufs of this transaction
func (buftxn *BufTxn) CommitWait(wait bool, abort bool) bool {
	util.DPrintf(1, "Commit %d w %v a %v\n", buftxn.Id, wait, abort)
	ok := buftxn.txn.CommitWait(buftxn.bufs.DirtyBufs(), wait, abort, buftxn.Id)
	return ok
}

func (buftxn *BufTxn) Flush() bool {
	ok := buftxn.txn.Flush()
	return ok
}
