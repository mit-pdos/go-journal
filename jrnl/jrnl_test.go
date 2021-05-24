package jrnl_test

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/mit-pdos/go-journal/addr"
	"github.com/mit-pdos/go-journal/jrnl"
	"github.com/mit-pdos/go-journal/obj"
	"github.com/mit-pdos/go-journal/util"
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

const inodeSz uint64 = 8 * 128

func inodeAddr(i uint64) addr.Addr {
	return addr.MkAddr(513+i/32, (i%32)*inodeSz)
}

func TestJrnlWriteRead(t *testing.T) {
	d := disk.NewMemDisk(10000)
	log := obj.MkLog(d)

	op := jrnl.Begin(log)
	bs0 := data(128)
	bs1 := data(128)
	op.OverWrite(inodeAddr(0), inodeSz, bs0)
	op.OverWrite(inodeAddr(1), inodeSz, bs1)
	op.CommitWait(true)

	op = jrnl.Begin(log)
	buf := op.ReadBuf(inodeAddr(0), inodeSz)
	assert.Equal(t, bs0, buf.Data)
	buf = op.ReadBuf(inodeAddr(1), inodeSz)
	assert.Equal(t, bs1, buf.Data)
}

func assertObj(t *testing.T, expected []byte, op *jrnl.Op, a addr.Addr,
	msgAndArgs ...interface{}) {
	t.Helper()
	sz := 8 * uint64(len(expected))
	buf := op.ReadBuf(a, sz)
	assert.Equal(t, expected, buf.Data, msgAndArgs...)
}

func TestJrnlReadSetDirty(t *testing.T) {
	d := disk.NewMemDisk(10000)
	log := obj.MkLog(d)

	op := jrnl.Begin(log)
	// initialize with non-zero data
	bs0 := data(128)
	bs1 := data(128)
	op.OverWrite(inodeAddr(0), inodeSz, util.CloneByteSlice(bs0))
	op.OverWrite(inodeAddr(1), inodeSz, util.CloneByteSlice(bs1))
	op.CommitWait(true)
	log.Shutdown()

	log = obj.MkLog(d)
	op = jrnl.Begin(log)
	// modify just inode 1 through ReadBuf
	buf := op.ReadBuf(inodeAddr(1), inodeSz)
	buf.Data[0], buf.Data[1] = 0, 0
	buf.SetDirty()
	op.CommitWait(true)

	bs1[0], bs1[1] = 0, 0
	assertObj(t, bs0, op, inodeAddr(0), "inode 0 should be unaffected")
	assertObj(t, bs1, op, inodeAddr(1))
}

func testJrnlConcurrentOperations(t *testing.T, wait bool) {
	d := disk.NewMemDisk(10000)
	log := obj.MkLog(d)

	// 2048 = 64*32, so 64 blocks worth of "inodes"
	const numInodes = 2048

	inodes := make([][]byte, numInodes)
	var wg sync.WaitGroup
	wg.Add(numInodes)
	for i := uint64(0); i < numInodes; i++ {
		i := i
		go func() {
			op := jrnl.Begin(log)
			bs := data(128)
			op.OverWrite(inodeAddr(i), inodeSz, bs)
			op.CommitWait(wait)
			inodes[i] = bs
			wg.Done()
		}()
	}
	wg.Wait()
	log.Flush()

	op := jrnl.Begin(log)
	for i := uint64(0); i < numInodes; i++ {
		assertObj(t, inodes[i], op, inodeAddr(i), "inode %d incorrect", i)
	}
}

func TestConcurrent(t *testing.T) {
	t.Run("synchronous", func(t *testing.T) {
		testJrnlConcurrentOperations(t, true)
	})
	t.Run("asynchronous", func(t *testing.T) {
		testJrnlConcurrentOperations(t, false)
	})
}
