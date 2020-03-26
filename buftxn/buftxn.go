package buftxn

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/addr"
	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/txn"
	"github.com/mit-pdos/goose-nfsd/util"
)

//
// Txn layer used by file system.  A transaction has buffers that it
// has read/written.
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

func (buftxn *BufTxn) ReadBuf(addr addr.Addr, sz uint64) *buf.Buf {
	b := buftxn.bufs.Lookup(addr)
	if b == nil {
		buf := buftxn.txn.Load(addr, sz)
		buftxn.bufs.Insert(buf)
		return buftxn.bufs.Lookup(addr)
	}
	return b
}

// Caller overwrites addr without reading it
func (buftxn *BufTxn) OverWrite(addr addr.Addr, sz uint64, data []byte) {
	var b = buftxn.bufs.Lookup(addr)
	if b == nil {
		b = buf.MkBuf(addr, sz, data)
		buftxn.bufs.Insert(b)
	} else {
		if sz != b.Sz {
			panic("overwrite")
		}
		b.Data = data
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
func (buftxn *BufTxn) CommitWait(wait bool) bool {
	util.DPrintf(1, "Commit %d w %v\n", buftxn.Id, wait)
	ok := buftxn.txn.CommitWait(buftxn.bufs.DirtyBufs(), wait, buftxn.Id)
	return ok
}

func (buftxn *BufTxn) Flush() bool {
	ok := buftxn.txn.Flush()
	return ok
}
