package replicated_block

import (
	"testing"

	"github.com/goose-lang/primitive/disk"
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
	tx := obj.MkLog(d)
	rb := Open(tx, 514)
	ok := rb.Write(mkBlock(1))
	assert.True(t, ok, "write txn should succeed")

	b, ok := rb.Read()
	assert.True(t, ok, "read-only txn should succeed")
	assert.Equal(t, byte(1), b[0])

	tx.Shutdown()
}

func TestRepBlockRecovery(t *testing.T) {
	d := disk.NewMemDisk(1000)
	tx := obj.MkLog(d)
	rb := Open(tx, 514)
	ok := rb.Write(mkBlock(1))
	assert.True(t, ok, "write txn should succeed")
	tx.Shutdown()

	tx2 := obj.MkLog(d)
	rb2 := Open(tx2, 514)
	b, _ := rb2.Read()
	assert.Equal(t, byte(1), b[0], "rep block should be crash safe")
	tx2.Shutdown()
}
