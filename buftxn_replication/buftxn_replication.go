package replicated_block

import (
	"sync"

	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/addr"
	"github.com/mit-pdos/goose-nfsd/buftxn"
	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/txn"
	"github.com/mit-pdos/goose-nfsd/util"
)

type RepBlock struct {
	txn *txn.Txn

	m  *sync.Mutex
	a0 addr.Addr
	a1 addr.Addr
}

func Open(txn *txn.Txn, a common.Bnum) *RepBlock {
	return &RepBlock{
		txn: txn,
		m:   new(sync.Mutex),
		a0:  addr.MkAddr(a, 0),
		a1:  addr.MkAddr(a+1, 0),
	}
}

// can fail in principle if CommitWait fails,
// but that's really impossible since it's an empty transaction
func (rb *RepBlock) Read() (disk.Block, bool) {
	rb.m.Lock()
	tx := buftxn.Begin(rb.txn)
	buf := tx.ReadBuf(rb.a0, 8*disk.BlockSize)
	b := util.CloneByteSlice(buf.Data)
	// now we can reassemble the transaction
	ok := tx.CommitWait(true)
	rb.m.Unlock()
	return b, ok
}

func (rb *RepBlock) Write(b disk.Block) bool {
	rb.m.Lock()
	tx := buftxn.Begin(rb.txn)
	tx.OverWrite(rb.a0, 8*disk.BlockSize, b)
	tx.OverWrite(rb.a1, 8*disk.BlockSize, b)
	ok := tx.CommitWait(true)
	rb.m.Unlock()
	return ok
}
