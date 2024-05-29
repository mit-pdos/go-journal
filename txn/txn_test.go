package txn_test

import (
	"math/rand"
	"testing"

	"github.com/mit-pdos/go-journal/addr"
	"github.com/mit-pdos/go-journal/disk"
	"github.com/mit-pdos/go-journal/txn"
	"github.com/mit-pdos/go-journal/wal"
	"github.com/stretchr/testify/assert"
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

const blockSz uint64 = 8 * disk.BlockSize

func TestReadWrite(t *testing.T) {
	d := disk.NewMemDisk(10000)
	tsys, err := txn.Init(d)
	assert.Nil(t, err)

	x := data(int(disk.BlockSize))

	tx := txn.Begin(tsys)
	tx.OverWrite(blockAddr(wal.LOGDISKBLOCKS), blockSz, x)
	tx.Commit(true)

	tx = txn.Begin(tsys)
	buf, err := tx.ReadBuf(blockAddr(wal.LOGDISKBLOCKS), blockSz)
	assert.Nil(t, err)
	assert.Equal(t, x, buf, "read incorrect data")
	tx.ReleaseAll()
}

func TestReadWriteAsync(t *testing.T) {
	d := disk.NewMemDisk(10000)
	tsys, err := txn.Init(d)
	assert.Nil(t, err)

	x := data(int(disk.BlockSize))

	tx := txn.Begin(tsys)
	tx.OverWrite(blockAddr(wal.LOGDISKBLOCKS), blockSz, x)
	tx.Commit(false)
	tsys.Flush()

	tx = txn.Begin(tsys)
	buf, err := tx.ReadBuf(blockAddr(wal.LOGDISKBLOCKS), blockSz)
	assert.Nil(t, err)
	assert.Equal(t, x, buf, "read incorrect data")
	tx.ReleaseAll()
}
