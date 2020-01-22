package buf

//
// A map from addr to an object.  It assumes that addr's don't overlap.  Implementation
// limits disk size to 2^64 bits.
//

type aentry struct {
	addr Addr
	obj  interface{}
}

type AddrMap map[uint64]*aentry

func MkAddrMap() *AddrMap {
	a := AddrMap(make(map[uint64]*aentry))
	return &a
}

func (amap *AddrMap) Len() uint64 {
	return uint64(len(*amap))
}

func (amap *AddrMap) Lookup(addr Addr) interface{} {
	var obj interface{}
	a, ok := (*amap)[addr.Flatid()]
	if ok {
		obj = a.obj
	}
	return obj
}

func (amap *AddrMap) Insert(addr Addr, obj interface{}) {
	aentry := &aentry{addr: addr, obj: obj}
	(*amap)[addr.Flatid()] = aentry
}

func (amap *AddrMap) Del(addr Addr) {
	index := addr.Flatid()
	_, found := (*amap)[addr.Flatid()]
	if !found {
		return
	}
	delete(*amap, index)
}

func (amap *AddrMap) Apply(f func(Addr, interface{})) {
	for _, a := range *amap {
		f(a.addr, a.obj)
	}
}
