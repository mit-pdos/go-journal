package replicated_block

import (
	"testing"

	"github.com/mit-pdos/go-journal/disk"
	"github.com/stretchr/testify/assert"

	"github.com/mit-pdos/go-journal/obj"
)

func mkBlock(b0 byte) disk.Block {
	b := make(disk.Block, disk.BlockSize)
	b[0] = b0
	return b
}

func TestRepBlock(t *testing.T) {
	d := disk.NewMemDisk(1000)
	tx, err := obj.MkLog(d)
	assert.Nil(t, err)
	rb := Open(tx, 514)
	err = rb.Write(mkBlock(1))
	assert.Nil(t, err, "write txn should succeed")

	b, err := rb.Read()
	assert.Nil(t, err, "read-only txn should succeed")
	assert.Equal(t, byte(1), b[0])

	tx.Shutdown()
}

func TestRepBlockRecovery(t *testing.T) {
	d := disk.NewMemDisk(1000)
	tx, err := obj.MkLog(d)
	assert.Nil(t, err)
	rb := Open(tx, 514)
	err = rb.Write(mkBlock(1))
	assert.Nil(t, err, "write txn should succeed")
	tx.Shutdown()

	tx2, err := obj.MkLog(d)
	assert.Nil(t, err)
	rb2 := Open(tx2, 514)
	b, err := rb2.Read()
	assert.Nil(t, err)
	assert.Equal(t, byte(1), b[0], "rep block should be crash safe")
	tx2.Shutdown()
}
