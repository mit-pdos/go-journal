package alloc

import (
	"sync"

	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/buftxn"
	"github.com/mit-pdos/goose-nfsd/util"
)

const (
	NBITBLOCK uint64 = disk.BlockSize * 8
)

// Allocator uses a bit map to allocate and free numbers. Bit 0
// corresponds to number 1, bit 1 to 1, and so on.
type Alloc struct {
	lock  *sync.Mutex // protects next
	start uint64
	len   uint64
	next  uint64 // first number to try
}

func MkAlloc(start uint64, len uint64) *Alloc {
	a := &Alloc{
		lock:  new(sync.Mutex),
		start: start,
		len:   len,
		next:  0,
	}
	return a
}

// Free bit bn in buf
func freeBit(buf *buf.Buf, bn uint64) {
	if bn != buf.Addr.Off {
		panic("freeBit")
	}
	bit := bn % 8
	buf.Blk[0] = buf.Blk[0] & ^(1 << bit)
}

func (a *Alloc) incNext(inc uint64) uint64 {
	a.lock.Lock()
	a.next = a.next + inc // inc bits
	if a.next >= a.len*NBITBLOCK {
		a.next = 0
	}
	num := a.next
	a.lock.Unlock()
	return num
}

// Returns a locked region in the bitmap with some free bits.
func (a *Alloc) findFreeRegion(buftxn *buftxn.BufTxn) *buf.Buf {
	var buf *buf.Buf
	var num uint64
	num = a.incNext(1)
	start := num
	for {
		b := a.lockRegion(buftxn, num, 1)
		bit := num % 8
		util.DPrintf(10, "findregion: s %d buf %v num %d byte 0x%x\n", start, b,
			num, b.Blk[0])
		if b.Blk[0]&(1<<bit) == 0 {
			b.Blk[0] = b.Blk[0] | (1 << bit)
			buf = b
			break
		}
		buftxn.Release(b.Addr)
		num = a.incNext(1)
		if num == start {
			return nil
		}
		continue
	}
	return buf
}

// Lock the region in the bitmap that contains n
func (a *Alloc) lockRegion(buftxn *buftxn.BufTxn, n uint64, bits uint64) *buf.Buf {
	var b *buf.Buf
	i := n / NBITBLOCK
	bit := n % NBITBLOCK
	addr := buf.MkAddr(a.start+i, bit, bits)
	b = buftxn.ReadBufLocked(addr)
	util.DPrintf(15, "LockRegion: %v\n", b)
	return b
}

func (a *Alloc) free(buf *buf.Buf, n uint64) {
	i := n / NBITBLOCK
	if i >= a.len {
		panic("freeBlock")
	}
	if buf.Addr.Blkno != a.start+i {
		panic("freeBlock")
	}
	freeBit(buf, n%NBITBLOCK)
}

func (a *Alloc) AllocNum(buftxn *buftxn.BufTxn) uint64 {
	var num uint64 = 0
	b := a.findFreeRegion(buftxn)
	if b != nil {
		b.SetDirty()
		num = (b.Addr.Blkno-a.start)*NBITBLOCK + b.Addr.Off
	}
	return num
}

func (a *Alloc) FreeNum(buftxn *buftxn.BufTxn, num uint64) {
	if num == 0 {
		panic("FreeNum")
	}
	buf := a.lockRegion(buftxn, num, 1)
	a.free(buf, num)
	buf.SetDirty()
}
