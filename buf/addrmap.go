package buf

//
// a map from addr to an object
//

type aentry struct {
	addr Addr
	obj  interface{}
}

type AddrMap struct {
	nelem uint64
	addrs map[uint64][]*aentry
}

func MkAddrMap() *AddrMap {
	a := &AddrMap{
		nelem: 0,
		addrs: make(map[uint64][]*aentry),
	}
	return a
}

func (amap *AddrMap) Len() uint64 {
	return amap.nelem
}

func (amap *AddrMap) Lookup(addr Addr) interface{} {
	var obj interface{}
	addrs, ok := amap.addrs[addr.Blkno]
	if ok {
		for _, a := range addrs {
			if addr.Eq(a.addr) {
				obj = a.obj
				break
			}
		}
	}
	return obj
}

func (amap *AddrMap) Insert(addr Addr, obj interface{}) {
	aentry := &aentry{addr: addr, obj: obj}
	blkno := addr.Blkno
	amap.addrs[blkno] = append(amap.addrs[blkno], aentry)
	amap.nelem += 1
}

func (amap *AddrMap) Del(addr Addr) {
	var index uint64
	var found bool

	blkno := addr.Blkno
	locks, found := amap.addrs[blkno]
	if !found {
		return
	}
	for i, l := range locks {
		if l.addr.Eq(addr) {
			index = uint64(i)
			found = true
		}
	}
	if !found {
		return
	}
	locks = append(locks[0:index], locks[index+1:]...)
	amap.addrs[blkno] = locks
	if len(locks) == 0 {
		delete(amap.addrs, blkno)
	}
	amap.nelem -= 1
}

func (amap *AddrMap) Apply(f func(Addr, interface{})) {
	for _, addrs := range amap.addrs {
		for _, a := range addrs {
			f(a.addr, a.obj)
		}
	}
}
