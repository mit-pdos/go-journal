package buf

import (
	"fmt"

	"github.com/tchajed/goose/machine/disk"
	"github.com/tchajed/marshal"

	"github.com/mit-pdos/goose-nfsd/bcache"
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
	bytefirst := addr.Off / 8
	bytelast := (addr.Off + addr.Sz - 1) / 8
	data := blk[bytefirst : bytelast+1]
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
	copy(dst[dstoff/8:], src[:sz])
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
	bytefirst := buf.Addr.Off / 8
	bytelast := (buf.Addr.Off + buf.Addr.Sz - 1) / 8
	buf.Blk = blk[bytefirst : bytelast+1]
}

func (buf *Buf) WriteDirect(d *bcache.Bcache) {
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
	dec := marshal.NewDec(buf.Blk[off : off+8])
	return Bnum(dec.GetInt())
}

func (buf *Buf) BnumPut(off uint64, v Bnum) {
	enc := marshal.NewEnc(8)
	enc.PutInt(uint64(v))
	copy(buf.Blk[off:off+8], enc.Finish())
	buf.SetDirty()
}
