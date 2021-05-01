package buf

import (
	"github.com/mit-pdos/goose-nfsd/addr"
)

//
// A map from Addr's to bufs.
//

type BufMap struct {
	addrs map[uint64]*Buf
}

func MkBufMap() *BufMap {
	a := &BufMap{
		addrs: make(map[uint64]*Buf),
	}
	return a
}

func (bmap *BufMap) Insert(buf *Buf) {
	bmap.addrs[buf.Addr.Flatid()] = buf
}

func (bmap *BufMap) Lookup(addr addr.Addr) *Buf {
	return bmap.addrs[addr.Flatid()]
}

func (bmap *BufMap) Del(addr addr.Addr) {
	delete(bmap.addrs, addr.Flatid())
}

func (bmap *BufMap) Ndirty() uint64 {
	// in GooseLang this call assumes map has < 2^64 elements
	_ = uint64(len(bmap.addrs))
	var n uint64 = 0
	for _, buf := range bmap.addrs {
		if buf.dirty {
			n++
		}
	}
	return n
}

func (bmap *BufMap) DirtyBufs() []*Buf {
	var bufs []*Buf
	for _, buf := range bmap.addrs {
		if buf.dirty {
			bufs = append(bufs, buf)
		}
	}
	return bufs
}
