package alloc

import (
	"sync"

	"github.com/mit-pdos/goose-nfsd/util"
)

// Allocator uses a bit map to allocate and free numbers. Bit 0
// corresponds to number 0, bit 1 to 1, and so on.
type Alloc struct {
	mu     *sync.Mutex
	next   uint64 // first number to try
	bitmap []byte
}

// MkAlloc initializes with a bitmap.
//
// 0 bits correspond to free numbers and 1 bits correspond to in-use numbers.
func MkAlloc(bitmap []byte) *Alloc {
	a := &Alloc{
		mu:     new(sync.Mutex),
		next:   0,
		bitmap: bitmap,
	}
	return a
}

func (a *Alloc) MarkUsed(bn uint64) {
	a.mu.Lock()
	byte := bn / 8
	bit := bn % 8
	a.bitmap[byte] = a.bitmap[byte] | (1 << bit)
	a.mu.Unlock()
}

// MkMaxAlloc initializes an allocator to be fully free with a range of (0,
// max).
//
// Requires 0 < max and max % 8 == 0.
func MkMaxAlloc(max uint64) *Alloc {
	if !(0 < max && max%8 == 0) {
		panic("invalid max, must be at least 0 and divisible by 8")
	}
	bitmap := make([]byte, max/8)
	a := MkAlloc(bitmap)
	a.MarkUsed(0)
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

func popCnt(b byte) uint64 {
	var count uint64
	var x = b
	for i := uint64(0); i < 8; i++ {
		count += uint64(b & 1)
		x = x >> 1
	}
	return count
}

func (a *Alloc) NumFree() uint64 {
	a.mu.Lock()
	var count uint64
	for _, b := range a.bitmap {
		count += popCnt(b)
	}
	a.mu.Unlock()
	return count
}
