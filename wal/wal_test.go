package wal

import (
	"fmt"

	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
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
	d := disk.Read(blkno + fs.DataStart())
	checkData(t, d, expected)
}

func mkBuf(fs *fs.FsSuper, blkno uint64, data []byte) *buf.Buf {
	addr := fs.Block2addr(blkno + fs.DataStart())
	b := buf.MkBuf(addr, mkData(disk.BlockSize))
	return b
}

func TestRecoverNone(t *testing.T) {
	fmt.Printf("TestRecoverNone\n")

	fs := fs.MkFsSuper()

	b := mkBuf(fs, 0, mkData(disk.BlockSize))

	l := MkLog()
	l.Shutdown()

	_, ok := l.MemAppend([]*buf.Buf{b})
	assert.Equal(t, ok, true)

	checkBlk(t, fs, 0, make([]byte, disk.BlockSize))

	l.recover()

	checkBlk(t, fs, 0, make([]byte, disk.BlockSize))
}

func TestRecoverSimple(t *testing.T) {
	fmt.Printf("TestRecoverSimple\n")

	fs := fs.MkFsSuper()
	d := mkData(disk.BlockSize)

	b := mkBuf(fs, 0, d)

	l := MkLog()

	txn, ok := l.MemAppend([]*buf.Buf{b})
	assert.Equal(t, ok, true)
	l.LogAppendWait(txn)

	l.Shutdown()

	l.recover()

	checkBlk(t, fs, 0, d)

}
