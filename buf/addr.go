package buf

// Address of disk object and its size
type Addr struct {
	Blkno uint64
	Off   uint64 // offset in bits
	Sz    uint64 // sz in bits
}

func (a *Addr) eq(b Addr) bool {
	return a.Blkno == b.Blkno && a.Off == b.Off && a.Sz == b.Sz
}

func MkAddr(blkno uint64, off uint64, sz uint64) Addr {
	return Addr{Blkno: blkno, Off: off, Sz: sz}
}
