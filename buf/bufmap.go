package buf

//
// A map from Addr's to bufs.
//

type BufMap struct {
	addrs *AddrMap
}

func MkBufMap() *BufMap {
	a := &BufMap{
		addrs: MkAddrMap(),
	}
	return a
}

func (bmap *BufMap) Insert(buf *Buf) {
	bmap.addrs.Insert(buf.Addr, buf)
}

func (bmap *BufMap) Lookup(addr Addr) *Buf {
	e := bmap.addrs.Lookup(addr)
	if e != nil {
		return e.(*Buf)
	}
	return nil
}

func (bmap *BufMap) Del(addr Addr) {
	bmap.addrs.Del(addr)
}

func (bmap *BufMap) Ndirty() uint64 {
	n := uint64(0)
	bmap.addrs.Apply(func(a Addr, e interface{}) {
		buf := e.(*Buf)
		if buf.dirty {
			n += 1
		}
	})
	return n
}

func (bmap *BufMap) Bufs() []*Buf {
	bufs := make([]*Buf, 0)
	bmap.addrs.Apply(func(a Addr, e interface{}) {
		b := e.(*Buf)
		bufs = append(bufs, b)
	})
	return bufs
}
