package wal

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/fake-bcache/bcache"
)

type WalSuite struct {
	suite.Suite
	d disk.Disk
	l *Walog
}

func (suite *WalSuite) SetupTest() {
	suite.d = disk.NewMemDisk(100000)
	disk.Init(suite.d)
	cache := bcache.MkBcache()
	suite.l = MkLog(cache)
}

func (suite *WalSuite) restart() *Walog {
	suite.l.Shutdown()
	cache := bcache.MkBcache()
	suite.l = MkLog(cache)
	return suite.l
}

func TestWal(t *testing.T) {
	suite.Run(t, new(WalSuite))
}

func (suite *WalSuite) checkMemAppend(txn []BlockData) {
	_, ok := suite.l.MemAppend(txn)
	suite.Equalf(true, ok,
		"mem append of %v blocks failed", len(txn))
}

func mkBlock(b byte) disk.Block {
	block := make(disk.Block, disk.BlockSize)
	for i := range block {
		block[i] = b
	}
	return block
}

var block0 = mkBlock(0)
var block1 = mkBlock(1)
var block2 = mkBlock(2)

func (suite *WalSuite) TestMemReadWrite() {
	l := suite.l
	l.MemAppend([]BlockData{
		MkBlockData(2, block2),
		MkBlockData(1, block1),
	})
	suite.Equal(block1, l.Read(1))
	suite.Equal(block2, l.Read(2))
	suite.Equal(block0, l.Read(3))
}

func (suite *WalSuite) TestMultiTxnReadWrite() {
	l := suite.l
	l.MemAppend([]BlockData{
		MkBlockData(2, block2),
		MkBlockData(3, block2),
	})
	l.MemAppend([]BlockData{
		MkBlockData(1, block2),
		MkBlockData(4, block2),
	})
	suite.Equal(block2, l.Read(1))
	suite.Equal(block2, l.Read(4))
	suite.Equal(block0, l.Read(0))
}

func (suite *WalSuite) TestFlush() {
	l := suite.l
	l.MemAppend([]BlockData{
		MkBlockData(2, block1),
		MkBlockData(1, block1),
	})
	l.WaitFlushMemLog()
	l.MemAppend([]BlockData{
		MkBlockData(3, block1),
		MkBlockData(2, block2),
	})
	suite.Equal(block1, l.Read(1))
	suite.Equal(block2, l.Read(2),
		"memory should overwrite disk log")
	suite.Equal(block1, l.Read(3))
}

// contiguousTxn gives a transaction that writes b to addresses [start,
// numWrites)
func contiguousTxn(start uint64, numWrites int, b disk.Block) []BlockData {
	var txn []BlockData
	for i := 0; i < numWrites; i++ {
		a := start + uint64(i)
		txn = append(txn, MkBlockData(a, b))
	}
	return txn
}

func (suite *WalSuite) TestTxnOverflowingMemLog() {
	l := suite.l
	// leaves one address in the memLog
	suite.checkMemAppend(contiguousTxn(1, int(LOGSZ-1), block1))
	suite.checkMemAppend(contiguousTxn(LOGSZ+10, 2, block2))
	// when this finishes, the first transaction should be flushed
	suite.Equal(block1, l.Read(1),
		"first transaction should be on disk")
	suite.Equal(block2, l.Read(LOGSZ+10),
		"second transaction should at least be in memory")
}

func (suite *WalSuite) TestShutdownQuiescent() {
	l := suite.l
	l.Shutdown()
}

func (suite *WalSuite) TestShutdownFlushed() {
	l := suite.l
	suite.checkMemAppend(contiguousTxn(1, 3, block1))
	l.WaitFlushMemLog()
	l.Shutdown()
}

func (suite *WalSuite) TestShutdownInProgress() {
	l := suite.l
	l.MemAppend(contiguousTxn(1, 3, block1))
	l.MemAppend(contiguousTxn(1, 10, block2))
	suite.checkMemAppend(contiguousTxn(1, int(LOGSZ-3), block1))
	l.Shutdown()
}

func (suite *WalSuite) TestRecoverFlushed() {
	l := suite.l
	l.MemAppend(contiguousTxn(1, 3, block1))
	l.MemAppend(contiguousTxn(20, 10, block2))
	l.WaitFlushMemLog()

	l = suite.restart()
	suite.Equal(block1, l.Read(2))
	suite.Equal(block2, l.Read(20))
}
