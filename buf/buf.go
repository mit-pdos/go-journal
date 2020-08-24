// buf manages sub-block disk objects, to be packed into disk blocks
package buf

import (
	"github.com/tchajed/goose/machine/disk"
	"github.com/tchajed/marshal"

	"github.com/mit-pdos/goose-nfsd/addr"
	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/util"
)

// A Buf is a write to a disk object (inode, a bitmap bit, or disk block)
type Buf struct {
	Addr  addr.Addr
	Sz    uint64 // number of bits
	Data  []byte
	dirty bool // has this block been written to?
}

func MkBuf(addr addr.Addr, sz uint64, data []byte) *Buf {
	b := &Buf{
		Addr:  addr,
		Sz:    sz,
		Data:  data,
		dirty: false,
	}
	return b
}

// Load the bits of a disk block into a new buf, as specified by addr
func MkBufLoad(addr addr.Addr, sz uint64, blk disk.Block) *Buf {
	bytefirst := addr.Off / 8
	bytelast := (addr.Off + sz - 1) / 8
	data := blk[bytefirst : bytelast+1]
	b := &Buf{
		Addr:  addr,
		Sz:    sz,
		Data:  data,
		dirty: false,
	}
	return b
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
	util.DPrintf(1, "%v: install\n", buf.Addr)
	if buf.Sz == 1 {
		installBit(buf.Data, blk, buf.Addr.Off)
	} else if buf.Sz%8 == 0 && buf.Addr.Off%8 == 0 {
		installBytes(buf.Data, blk, buf.Addr.Off, buf.Sz)
	} else {
		panic("Install unsupported\n")
	}
	util.DPrintf(20, "install -> %v\n", blk)
}

// Load the bits of a disk block into buf, as specified by addr
func (buf *Buf) Load(sz uint64, blk disk.Block) {
	bytefirst := buf.Addr.Off / 8
	bytelast := (buf.Addr.Off + sz - 1) / 8
	buf.Sz = sz
	buf.Data = blk[bytefirst : bytelast+1]
}

func (buf *Buf) IsDirty() bool {
	return buf.dirty
}

func (buf *Buf) SetDirty() {
	buf.dirty = true
}

func (buf *Buf) WriteDirect(d disk.Disk) {
	buf.SetDirty()
	if buf.Sz == disk.BlockSize {
		d.Write(uint64(buf.Addr.Blkno), buf.Data)
	} else {
		blk := d.Read(uint64(buf.Addr.Blkno))
		buf.Install(blk)
		d.Write(uint64(buf.Addr.Blkno), blk)
	}
}

func (buf *Buf) BnumGet(off uint64) common.Bnum {
	dec := marshal.NewDec(buf.Data[off : off+8])
	return common.Bnum(dec.GetInt())
}

func (buf *Buf) BnumPut(off uint64, v common.Bnum) {
	enc := marshal.NewEnc(8)
	enc.PutInt(uint64(v))
	copy(buf.Data[off:off+8], enc.Finish())
	buf.SetDirty()
}
