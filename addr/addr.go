package addr

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/common"
)

// Address of disk object and its size
type Addr struct {
	Blkno common.Bnum
	Off   uint64 // offset in bits
	Sz    uint64 // sz in bits
}

func (a *Addr) Flatid() uint64 {
	return uint64(a.Blkno)*(disk.BlockSize*8) + a.Off
}

func (a *Addr) Eq(b Addr) bool {
	return a.Blkno == b.Blkno && a.Off == b.Off && a.Sz == b.Sz
}

func MkAddr(blkno common.Bnum, off uint64, sz uint64) Addr {
	return Addr{Blkno: blkno, Off: off, Sz: sz}
}

func MkBitAddr(start common.Bnum, n uint64) Addr {
	bit := n % common.NBITBLOCK
	i := n / common.NBITBLOCK
	addr := MkAddr(start+common.Bnum(i), bit, 1)
	return addr
}
