package addr

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/common"
)

// Addr identifies the start of a disk object.
//
// Blkno is the block number containing the object, and Off is the location of
// the object within the block (expressed as a bit offset). The size of the
// object is determined by the context in which Addr is used.
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
