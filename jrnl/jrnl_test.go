package jrnl_test

import (
	"math/rand"
	"testing"

	"github.com/mit-pdos/go-journal/addr"
	"github.com/mit-pdos/go-journal/jrnl"
	"github.com/mit-pdos/go-journal/obj"
	"github.com/mit-pdos/go-journal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/tchajed/goose/machine/disk"
)

func TestSizeConstants(t *testing.T) {
	assert.Equal(t, wal.LOGSZ, jrnl.LogBlocks)
	assert.Equal(t, disk.BlockSize*wal.LOGSZ, jrnl.LogBytes)
}

func data(sz int) []byte {
	d := make([]byte, sz)
	rand.Read(d)
	return d
}

const InodeSz uint64 = 8 * 128

func TestJrnlWriteRead(t *testing.T) {
	d := disk.NewMemDisk(10000)
	log := obj.MkLog(d)

	op := jrnl.Begin(log)
	a0 := addr.MkAddr(512, 0*InodeSz)
	a1 := addr.MkAddr(512, 1*InodeSz)
	bs0 := data(128)
	bs1 := data(128)
	op.OverWrite(a0, InodeSz, bs0)
	op.OverWrite(a1, InodeSz, bs1)
	op.CommitWait(true)

	op = jrnl.Begin(log)
	buf := op.ReadBuf(a0, InodeSz)
	assert.Equal(t, bs0, buf.Data)
	buf = op.ReadBuf(a1, InodeSz)
	assert.Equal(t, bs1, buf.Data)
}
