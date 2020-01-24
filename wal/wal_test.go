package wal

import (
	"fmt"

	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/fs"

	"testing"

	"github.com/stretchr/testify/assert"
)

func mkData(sz uint64) []byte {
	data := make([]byte, sz)
	for i := range data {
		data[i] = byte(i % 128)
	}
	return data
}

func checkData(t *testing.T, read, expected []byte) {
	assert.Equal(t, len(read), len(expected))
	for i := uint64(0); i < uint64(len(read)); i++ {
		assert.Equal(t, read[i], expected[i])
	}
}

func checkBlk(t *testing.T, fs *fs.FsSuper, blkno uint64, expected []byte) {
	d := fs.Disk.Read(blkno + fs.DataStart())
	checkData(t, d, expected)
}

func TestRecoverNone(t *testing.T) {
	fmt.Printf("TestRecoverNone\n")
	fs := fs.MkFsSuper(100*1000, nil)

	b := MkBlockData(0, mkData(disk.BlockSize))

	l := MkLog(fs.Disk)
	l.Shutdown()

	_, ok := l.MemAppend([]BlockData{b})
	assert.Equal(t, ok, true)

	checkBlk(t, fs, 0, make([]byte, disk.BlockSize))

	l.recover()

	checkBlk(t, fs, 0, make([]byte, disk.BlockSize))
}

func TestRecoverSimple(t *testing.T) {
	fmt.Printf("TestRecoverSimple\n")
	fs := fs.MkFsSuper(100*1000, nil)
	d := mkData(disk.BlockSize)

	b := MkBlockData(fs.DataStart(), d)

	l := MkLog(fs.Disk)

	txn, ok := l.MemAppend([]BlockData{b})
	assert.Equal(t, ok, true)
	l.LogAppendWait(txn)

	l.Shutdown()

	l.recover()

	checkBlk(t, fs, 0, d)
}
