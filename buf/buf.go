package buf

import (
	"fmt"

	"github.com/tchajed/goose/machine"
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/util"
)

// A buf holds a disk object (inode, a bitmap bit, or disk block)
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

// Load the bits of a disk block into a new buf, as specified by addr
func MkBufLoad(addr Addr, blk disk.Block) *Buf {
	byte := addr.Off / 8
	sz := util.RoundUp(addr.Sz, 8)
	data := blk[byte : byte+sz]
	b := &Buf{
		Addr:  addr,
		Blk:   data,
		dirty: false,
	}
	return b
}

func (buf *Buf) String() string {
	return fmt.Sprintf("%v %v", buf.Addr, buf.dirty)
}

// Install 1 bit from src into dst, at offset bit. return new dst.
func installOneBit(src byte, dst byte, bit uint64) byte {
	var new byte = dst
	if src&(1<<bit) != dst&(1<<bit) {
		if src&(1<<bit) == 0 {
			// dst is 1, but should be 0
			new = new & ^(1 << bit)
		} else {
			// dst is 0, but should be 1
			new = new | (1 << bit)
		}
	}
	return new
}

// Install bit from src to dst, at dstoff in destination. dstoff is in bits.
func installBit(src []byte, dst []byte, dstoff uint64) {
	dstbyte := dstoff / 8
	dst[dstbyte] = installOneBit(src[0], dst[dstbyte], (dstoff)%8)
}

// Install bytes from src to dst.
func installBytes(src []byte, dst []byte, dstoff uint64, nbit uint64) {
	sz := nbit / 8
	for i := uint64(0); i < sz; i++ {
		dst[(dstoff/8)+i] = src[i]
	}
}

// Install the bits from buf into blk.  Two cases: a bit or an inode
func (buf *Buf) Install(blk disk.Block) {
	util.DPrintf(20, "%v: install %v\n", buf.Addr, blk)
	if buf.Addr.Sz == 1 {
		installBit(buf.Blk, blk, buf.Addr.Off)
	} else if buf.Addr.Sz%8 == 0 && buf.Addr.Off%8 == 0 {
		installBytes(buf.Blk, blk, buf.Addr.Off, buf.Addr.Sz)
	} else {
		panic("Install unsupported\n")
	}
	util.DPrintf(20, "install -> %v\n", blk)
}

// Load the bits of a disk block into buf, as specified by addr
func (buf *Buf) Load(blk disk.Block) {
	byte := buf.Addr.Off / 8
	sz := util.RoundUp(buf.Addr.Sz, 8)
	buf.Blk = blk[byte : byte+sz]
}

func (buf *Buf) WriteDirect(d disk.Disk) {
	buf.SetDirty()
	if buf.Addr.Sz == disk.BlockSize {
		d.Write(uint64(buf.Addr.Blkno), buf.Blk)
	} else {
		blk := d.Read(uint64(buf.Addr.Blkno))
		buf.Install(blk)
		d.Write(uint64(buf.Addr.Blkno), blk)
	}
}

func (buf *Buf) IsDirty() bool {
	return buf.dirty
}

func (buf *Buf) SetDirty() {
	buf.dirty = true
}

func (buf *Buf) BnumGet(off uint64) Bnum {
	return Bnum(machine.UInt64Get(buf.Blk[off : off+8]))
}

func (buf *Buf) BnumPut(off uint64, v Bnum) {
	machine.UInt64Put(buf.Blk[off:off+8], uint64(v))
	buf.SetDirty()
}
