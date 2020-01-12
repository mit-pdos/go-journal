package buf

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/util"

	"fmt"
)

// A buf holds the disk object (inode, bitmap block, etc.) at Addr.
type Buf struct {
	Addr  Addr
	Blk   disk.Block
	dirty bool // has this block been written to?
}

func MkBuf(addr Addr, blk disk.Block) *Buf {
	if uint64(len(blk)) > disk.BlockSize {
		panic("mkbuf")
	}
	b := &Buf{
		Addr:  addr,
		Blk:   blk,
		dirty: false,
	}
	return b
}

func MkBufData(addr Addr) *Buf {
	sz := util.RoundUp(addr.Sz, 8)
	data := make([]byte, sz)
	buf := MkBuf(addr, data)
	return buf
}

func (buf *Buf) String() string {
	return fmt.Sprintf("%v %v", buf.Addr, buf.dirty)
}

// copy nbit bits from src to dst, starting to bit. return new dst.
func installBits(src byte, dst byte, bit uint64, nbit uint64) byte {
	var new byte = dst
	for i := bit; i < bit+nbit; i++ {
		if src&(1<<i) == dst&(1<<i) {
			continue
		}
		if src&(1<<i) == 0 {
			// dst is 1, but should be 0
			new = new & ^(1 << i)
		} else {
			// dst is 0, but should be 1
			new = new | (1 << i)
		}
	}
	return new
}

// copy nbits from src to dst, at dstoff in destination. dstoff is in bits.
func copyBits(src []byte, dst []byte, dstoff uint64, nbit uint64) {
	for i := uint64(0); i < nbit; i++ {
		dstbyte := (dstoff + i) / 8
		dst[dstbyte] = installBits(src[i/8], dst[dstbyte], (dstoff+i)%8, 1)
	}
}

// copy nbits from src to dst. dstoff is byte aligned, so can copy byte at
// the time
func copyBitsAligned(src []byte, dst []byte, dstoff uint64, nbit uint64) {
	sz := nbit / 8
	for i := uint64(0); i < sz; i++ {
		dst[(dstoff/8)+i] = src[i]
	}
	nbit -= sz * 8
	// copy few remaining bits
	copyBits(src[sz:], dst[(dstoff/8)+sz:], 0, nbit)
}

// Install the bits from buf into blk, if buf has been modified
func (buf *Buf) Install(blk disk.Block) {
	util.DPrintf(20, "install %v\n", blk)
	if buf.Addr.Off%8 == 0 {
		copyBitsAligned(buf.Blk, blk, buf.Addr.Off, buf.Addr.Sz)
	} else {
		copyBits(buf.Blk, blk, buf.Addr.Off, buf.Addr.Sz)
	}
	util.DPrintf(20, "install -> %v\n", blk)
}

// Load the bits of a disk block into buf, as specified by addr
func (buf *Buf) Load(blk disk.Block) {
	byte := buf.Addr.Off / 8
	sz := util.RoundUp(buf.Addr.Sz, 8)
	buf.Blk = blk[byte : byte+sz]
	// copy(buf.Blk, blk[byte:byte+sz])
}

func (buf *Buf) WriteDirect() {
	buf.SetDirty()
	if buf.Addr.Sz == disk.BlockSize {
		disk.Write(buf.Addr.Blkno, buf.Blk)
	} else {
		blk := disk.Read(buf.Addr.Blkno)
		buf.Install(blk)
		disk.Write(buf.Addr.Blkno, blk)
	}
}

func (buf *Buf) IsDirty() bool {
	return buf.dirty
}

func (buf *Buf) SetDirty() {
	buf.dirty = true
}
