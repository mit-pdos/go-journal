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

func installBits(src byte, dst byte, bit uint64, nbit uint64) byte {
	util.DPrintf(20, "installBits: src 0x%x dst 0x%x %d sz %d\n", src, dst, bit, nbit)
	var new byte = dst
	for i := bit; i < bit+nbit; i++ {
		if src&(1<<i) == dst&(1<<i) {
			continue
		}
		if src&(1<<i) == 0 {
			// dst is 1, but should be 0
			new = new & ^(1 << bit)
		} else {
			// dst is 0, but should be 1
			new = new | (1 << bit)
		}
	}
	util.DPrintf(20, "installBits -> 0x%x\n", new)
	return new
}

// copy nbits from src to dst, at dstoff in destination. dstoff is in bits.
func copyBits(src []byte, dst []byte, dstoff uint64, nbit uint64) {
	var n uint64 = nbit
	var off uint64 = 0
	var dstbyte uint64 = dstoff / 8

	// copy few last bits in first byte, if not byte aligned
	if dstoff%8 != 0 {
		bit := dstoff % 8
		nbit := util.Min(8-bit, n)
		srcbyte := src[0]
		// TODO: which of these should be dstbyte vs dstbyte2?
		dstbyte2 := dst[dstbyte]
		dst[dstbyte2] = installBits(srcbyte, dstbyte2, bit, nbit)
		off += 8
		dstbyte += 1
		n -= nbit
	}

	// copy bytes
	sz := n / 8
	for i := off; i < off+sz; i++ {
		dst[i+dstbyte] = src[i]
	}
	n -= sz * 8
	off += sz * 8

	// copy remaining bits
	if n > 0 {
		lastbyte := off / 8
		srcbyte := src[lastbyte]
		dstbyte := dst[lastbyte+dstbyte]
		dst[lastbyte] = installBits(srcbyte, dstbyte, 0, n)
	}

}

// Install the bits from buf into blk, if buf has been modified
func (buf *Buf) Install(blk disk.Block) {
	copyBits(buf.Blk, blk, buf.Addr.Off, buf.Addr.Sz)
}

// Load the bits of a disk block into buf, as specified by addr
func (buf *Buf) Load(blk disk.Block) {
	byte := buf.Addr.Off / 8
	sz := util.RoundUp(buf.Addr.Sz, 8)
	util.DPrintf(0, "Load %v sz %v", buf.Addr, sz)
	copy(buf.Blk, blk[byte:byte+sz])
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
