package buftxn

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/txn"
	"github.com/mit-pdos/goose-nfsd/util"
)

type BufTxn struct {
	txn  *txn.Txn
	bufs *buf.BufMap // map of bufs read/written by trans
	id   txn.TransId
}

func Begin(txn *txn.Txn) *BufTxn {
	trans := &BufTxn{
		txn:  txn,
		bufs: buf.MkBufMap(),
		id:   txn.GetTransId(),
	}
	return trans
}

func (buftxn *BufTxn) ReadBufLocked(addr buf.Addr) *buf.Buf {
	util.DPrintf(10, "ReadBufLocked: %v\n", addr)

	// does this transaction already have addr locked?  (e.g.,
	// read the inode from the inode cache, after locking it)
	locked := buftxn.txn.IsLocked(addr, buftxn.id)
	if !locked {
		buftxn.txn.Acquire(addr, buftxn.id)
	}
	return buftxn.ReadBuf(addr)
}

func (buftxn *BufTxn) ReadBuf(addr buf.Addr) *buf.Buf {
	b := buftxn.bufs.Lookup(addr)
	if b == nil {
		buf := buftxn.txn.Load(addr)
		buftxn.bufs.Insert(buf)
	}
	b = buftxn.bufs.Lookup(addr)
	return b
}

// caller has disk object (e.g., from cache), so don't read disk
// object from disk if we don't have buf for it.
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

func (buftxn *BufTxn) Acquire(addr buf.Addr) {
	buftxn.txn.Acquire(addr, buftxn.id)
}

func (buftxn *BufTxn) Release(addr buf.Addr) {
	buftxn.bufs.Del(addr)
	buftxn.txn.Release(addr, buftxn.id)
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

// Commit bufs of this transaction
func (buftxn *BufTxn) CommitWait(wait bool, abort bool) bool {
	return buftxn.txn.CommitWait(buftxn.bufs.Bufs(), wait, abort, buftxn.id)
}

func (buftxn *BufTxn) Flush() bool {
	return buftxn.txn.Flush(buftxn.id)
}
