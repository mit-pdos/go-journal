package replicated_block

import (
	"sync"

	"github.com/mit-pdos/go-journal/disk"

	"github.com/mit-pdos/go-journal/addr"
	"github.com/mit-pdos/go-journal/common"
	"github.com/mit-pdos/go-journal/jrnl"
	"github.com/mit-pdos/go-journal/obj"
	"github.com/mit-pdos/go-journal/util"
)

type RepBlock struct {
	txn *obj.Log

	m  *sync.Mutex
	a0 addr.Addr
	a1 addr.Addr
}

func Open(txn *obj.Log, a common.Bnum) *RepBlock {
	return &RepBlock{
		txn: txn,
		m:   new(sync.Mutex),
		a0:  addr.MkAddr(a, 0),
		a1:  addr.MkAddr(a+1, 0),
	}
}

// can fail in principle if CommitWait fails,
// but that's really impossible since it's an empty transaction
func (rb *RepBlock) Read() (disk.Block, error) {
	rb.m.Lock()
	tx := jrnl.Begin(rb.txn)
	buf, err := tx.ReadBuf(rb.a0, 8*disk.BlockSize)
	if err != nil {
		return nil, err
	}
	b := util.CloneByteSlice(buf.Data)
	// now we can reassemble the transaction
	err = tx.CommitWait(true)
	rb.m.Unlock()
	return b, err
}

func (rb *RepBlock) Write(b disk.Block) error {
	rb.m.Lock()
	tx := jrnl.Begin(rb.txn)
	tx.OverWrite(rb.a0, 8*disk.BlockSize, b)
	tx.OverWrite(rb.a1, 8*disk.BlockSize, b)
	err := tx.CommitWait(true)
	rb.m.Unlock()
	return err
}
