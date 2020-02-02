package buftxn

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/addrlock"
	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/txn"
	"github.com/mit-pdos/goose-nfsd/util"
)

//
// Txn layer used by file system.  A transaction has locked bitmap
// addresses and buffers that it has read/written.
//

type BufTxn struct {
	txn   *txn.Txn
	bufs  *buf.BufMap // map of bufs read/written by this transaction
	Id    txn.TransId
	locks *addrlock.LockMap // a shared map of addresses to locks
	addrs []buf.Addr        // locked addrs of this transaction
}

func Begin(txn *txn.Txn, locks *addrlock.LockMap) *BufTxn {
	trans := &BufTxn{
		txn:   txn,
		bufs:  buf.MkBufMap(),
		Id:    txn.GetTransId(),
		locks: locks,
		addrs: make([]buf.Addr, 0),
	}
	util.DPrintf(1, "Begin: %v\n", trans.Id)
	return trans
}

func (buftxn *BufTxn) Acquire(addr buf.Addr) {
	buftxn.locks.Acquire(addr.Flatid(), buftxn.Id)
	buftxn.addrs = append(buftxn.addrs, addr)
}

func (buftxn *BufTxn) deladdr(addr buf.Addr) {
	for i, a := range buftxn.addrs {
		if addr.Eq(a) {
			buftxn.addrs[i] = buftxn.addrs[len(buftxn.addrs)-1]
			buftxn.addrs = buftxn.addrs[:len(buftxn.addrs)-1]
		}
	}
}

func (buftxn *BufTxn) Release(addr buf.Addr) {
	buftxn.bufs.Del(addr)
	buftxn.deladdr(addr)
	buftxn.locks.Release(addr.Flatid(), buftxn.Id)
}

func (buftxn *BufTxn) IsLocked(addr buf.Addr) bool {
	return buftxn.locks.IsLocked(addr.Flatid(), buftxn.Id)
}

func (buftxn *BufTxn) releaseTxn() {
	util.DPrintf(5, "releaseTxn: %d %v\n", buftxn.Id, buftxn.addrs)
	for _, a := range buftxn.addrs {
		buftxn.Release(a)
	}
}

// Use for reading bits in the bitmaps
func (buftxn *BufTxn) ReadBitLocked(addr buf.Addr) *buf.Buf {
	buftxn.Acquire(addr)
	util.DPrintf(10, "ReadBitLocked: %d %v\n", buftxn.Id, addr)
	return buftxn.ReadBuf(addr)
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
	buftxn.releaseTxn()
	return ok
}

func (buftxn *BufTxn) Flush() bool {
	ok := buftxn.txn.Flush()
	buftxn.releaseTxn()
	return ok
}
