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
	start buf.Bnum
	len   uint64
	next  uint64 // first number to try
}

func MkAlloc(start buf.Bnum, len uint64) *Alloc {
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

func (a *Alloc) incNext() uint64 {
	a.lock.Lock()
	a.next = a.next + 1
	if a.next >= a.len*NBITBLOCK {
		a.next = 0
	}
	num := a.next
	a.lock.Unlock()
	return num
}

// Returns a locked free bit in the bitmap
func (a *Alloc) findFreeBit(buftxn *buftxn.BufTxn) *buf.Buf {
	var buf *buf.Buf
	var num uint64
	num = a.incNext()
	start := num
	for {
		b, alreadylocked := a.lockBit(buftxn, num)
		bit := num % 8
		util.DPrintf(10, "findFreeBit: s %d buf %v num %d byte 0x%x\n", start, b,
			num, b.Blk[0])
		if b.Blk[0]&(1<<bit) == 0 {
			b.Blk[0] = b.Blk[0] | (1 << bit)
			buf = b
			break
		}
		if !alreadylocked {
			buftxn.Release(b.Addr)
		}
		num = a.incNext()
		if num == start {
			return nil
		}
		continue
	}
	return buf
}

// Lock the n-th bit in the bitmap
func (a *Alloc) lockBit(buftxn *buftxn.BufTxn, n uint64) (*buf.Buf, bool) {
	var b *buf.Buf
	var alreadylocked = false
	i := n / NBITBLOCK
	bit := n % NBITBLOCK
	addr := buf.MkAddr(a.start+buf.Bnum(i), bit, 1)
	if buftxn.IsLocked(addr) {
		b = buftxn.ReadBuf(addr)
		alreadylocked = true
	} else {
		b = buftxn.ReadBufLocked(addr)
	}
	util.DPrintf(15, "lockBit: %v\n", b)
	return b, alreadylocked
}

func (a *Alloc) free(b *buf.Buf, n uint64) {
	i := n / NBITBLOCK
	if i >= a.len {
		panic("freeBlock")
	}
	if b.Addr.Blkno != a.start+buf.Bnum(i) {
		panic("freeBlock")
	}
	freeBit(b, n%NBITBLOCK)
}

func (a *Alloc) AllocNum(buftxn *buftxn.BufTxn) uint64 {
	var num uint64 = 0
	b := a.findFreeBit(buftxn)
	if b != nil {
		b.SetDirty()
		num = (uint64(b.Addr.Blkno-a.start))*NBITBLOCK + b.Addr.Off
	}
	return num
}

func (a *Alloc) FreeNum(buftxn *buftxn.BufTxn, num uint64) {
	if num == 0 {
		panic("FreeNum")
	}
	buf, _ := a.lockBit(buftxn, num)
	a.free(buf, num)
	buf.SetDirty()
}
