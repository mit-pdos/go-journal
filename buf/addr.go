package buf

import (
	"github.com/tchajed/goose/machine/disk"
)

type Bnum = uint64

const NULLBNUM Bnum = 0

// Address of disk object and its size
type Addr struct {
	Blkno Bnum
	Off   uint64 // offset in bits
	Sz    uint64 // sz in bits
}

func (a *Addr) Flatid() uint64 {
	return uint64(a.Blkno)*(disk.BlockSize*8) + a.Off
}

func (a *Addr) Eq(b Addr) bool {
	return a.Blkno == b.Blkno && a.Off == b.Off && a.Sz == b.Sz
}

func MkAddr(blkno Bnum, off uint64, sz uint64) Addr {
	return Addr{Blkno: blkno, Off: off, Sz: sz}
}
