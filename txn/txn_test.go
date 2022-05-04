package txn_test

import (
	"math/rand"
	"testing"

	"github.com/mit-pdos/go-journal/addr"
	"github.com/mit-pdos/go-journal/txn"
	"github.com/stretchr/testify/assert"
	"github.com/tchajed/goose/machine/disk"
)

func data(sz int) []byte {
	d := make([]byte, sz)
	rand.Read(d)
	return d
}

func blockAddr(a uint64) addr.Addr {
	return addr.Addr{
		Blkno: a,
		Off:   0,
	}
}

const blockSz uint64 = 8 * 4096

func TestReadWrite(t *testing.T) {
	d := disk.NewMemDisk(10000)
	tsys := txn.Init(d)

	x := data(4096)

	tx := txn.Begin(tsys)
	tx.OverWrite(blockAddr(513), blockSz, x)
	tx.Commit(true)

	tx = txn.Begin(tsys)
	buf := tx.ReadBuf(blockAddr(513), blockSz)
	assert.Equal(t, x, buf, "read incorrect data")
	tx.ReleaseAll()
}

func TestReadWriteAsync(t *testing.T) {
	d := disk.NewMemDisk(10000)
	tsys := txn.Init(d)

	x := data(4096)

	tx := txn.Begin(tsys)
	tx.OverWrite(blockAddr(513), blockSz, x)
	tx.Commit(false)
	tx.Flush()

	tx = txn.Begin(tsys)
	buf := tx.ReadBuf(blockAddr(513), blockSz)
	assert.Equal(t, x, buf, "read incorrect data")
	tx.ReleaseAll()
}
