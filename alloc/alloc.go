package alloc

import (
	"sync"

	"github.com/mit-pdos/goose-nfsd/util"
)

// Allocator uses a bit map to allocate and free numbers. Bit 0
// corresponds to number 1, bit 1 to 1, and so on.
type Alloc struct {
	mu     *sync.Mutex
	next   uint64 // first number to try
	bitmap []byte
}

func MkAlloc(bitmap []byte) *Alloc {
	a := &Alloc{
		mu:     new(sync.Mutex),
		next:   0,
		bitmap: bitmap,
	}
	return a
}

func (a *Alloc) incNext() uint64 {
	a.next = a.next + 1
	if a.next >= uint64(len(a.bitmap)*8) {
		a.next = 0
	}
	return a.next
}

// Returns a free number in the bitmap
func (a *Alloc) allocBit() uint64 {
	var num uint64
	a.mu.Lock()
	num = a.incNext()
	start := num
	for {
		bit := num % 8
		byte := num / 8
		util.DPrintf(10, "allocBit: s %d num %d\n", start, num)
		if a.bitmap[byte]&(1<<bit) == 0 {
			a.bitmap[byte] = a.bitmap[byte] | (1 << bit)
			break
		}
		num = a.incNext()
		if num == start { // looped around?
			num = 0
			break
		}
		continue
	}
	a.mu.Unlock()
	return num
}

func (a *Alloc) freeBit(bn uint64) {
	a.mu.Lock()
	byte := bn / 8
	bit := bn % 8
	a.bitmap[byte] = a.bitmap[byte] & ^(1 << bit)
	a.mu.Unlock()
}

func (a *Alloc) AllocNum() uint64 {
	num := a.allocBit()
	return num
}

func (a *Alloc) FreeNum(num uint64) {
	if num == 0 {
		panic("FreeNum")
	}
	a.freeBit(num)
}
