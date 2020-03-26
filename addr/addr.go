package addr

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/common"
)

// Address of disk object and its size
type Addr struct {
	Blkno common.Bnum
	Off   uint64 // offset in bits
}

func (a Addr) Flatid() uint64 {
	return uint64(a.Blkno)*(disk.BlockSize*8) + a.Off
}

func MkAddr(blkno common.Bnum, off uint64) Addr {
	return Addr{Blkno: blkno, Off: off}
}

func MkBitAddr(start common.Bnum, n uint64) Addr {
	bit := n % common.NBITBLOCK
	i := n / common.NBITBLOCK
	addr := MkAddr(start+common.Bnum(i), bit)
	return addr
}
