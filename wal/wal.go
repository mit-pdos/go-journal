package wal

import (
	"github.com/tchajed/goose/machine"
	"github.com/tchajed/goose/machine/disk"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/fs"
	"github.com/mit-pdos/goose-nfsd/marshal"
	"github.com/mit-pdos/goose-nfsd/util"

	"sync"
)

type TxnNum uint64

const LOGHDR = uint64(0)
const LOGSTART = uint64(1)

type Walog struct {
	// Protects in-memory-related log state
	memLock   *sync.Mutex
	logSz     uint64
	memLog    []buf.Buf // in-memory log [memTail,memHead)
	memTail   uint64    // tail of in-memory log

	// Protects disk-related log state, incl. header, diskHead,
	// shutdown
	logLock     *sync.Mutex
	condLogger  *sync.Cond
	condInstall *sync.Cond
	diskHead    TxnNum // next block to log to disk
	shutdown    bool
}

func MkLog() *Walog {
	ll := new(sync.Mutex)
	l := &Walog{
		memLock:     new(sync.Mutex),
		logLock:     ll,
		condLogger:  sync.NewCond(ll),
		condInstall: sync.NewCond(ll),
		logSz:       fs.HDRADDRS,
		memLog:      make([]buf.Buf, 0),
		memTail:     0,
		diskHead:    0,
		shutdown:    false,
	}
	util.DPrintf(1, "mkLog: size %d\n", l.logSz)

	l.recover()

	// TODO: do we still need to use machine.Spawn,
	//  or can we just use go statements?
	machine.Spawn(func() { l.logger() })
	machine.Spawn(func() { l.installer() })

	return l
}

// On-disk header in first block of log
type hdr struct {
	head      uint64
	tail      uint64
	addrs     []uint64
}

func decodeHdr(blk disk.Block) *hdr {
	hdr := &hdr{
		head:      0,
		tail:      0,
		addrs:     nil,
	}
	dec := marshal.NewDec(blk)
	hdr.head = dec.GetInt()
	hdr.tail = dec.GetInt()
	hdr.addrs = dec.GetInts(hdr.head - hdr.tail)
	return hdr
}

func encodeHdr(hdr hdr, blk disk.Block) {
	enc := marshal.NewEnc(blk)
	enc.PutInt(hdr.head)
	enc.PutInt(hdr.tail)
	enc.PutInts(hdr.addrs)
}

func (l *Walog) writeHdr(head uint64, tail uint64, bufs []buf.Buf) {
	n := uint64(len(bufs))
	addrs := make([]uint64, n)
	if n != head-tail {
		panic("writeHdr")
	}
	for i := tail; i < head; i++ {
		addrs[i-tail] = bufs[i-tail].Addr.Blkno
	}
	hdr := hdr{head: head, tail: tail, addrs: addrs}
	blk := make(disk.Block, disk.BlockSize)
	encodeHdr(hdr, blk)
	disk.Write(LOGHDR, blk)
}

func (l *Walog) readHdr() *hdr {
	blk := disk.Read(LOGHDR)
	hdr := decodeHdr(blk)
	return hdr
}

func (l *Walog) recover() {
	hdr := l.readHdr()
	for i := uint64(0); i < hdr.head - hdr.tail; i++ {
		util.DPrintf(1, "recover block %d\n", hdr.addrs[i])
		blk := disk.Read(LOGSTART + i)
		disk.Write(hdr.addrs[i], blk)
	}
	l.writeHdr(0, 0, []buf.Buf{})
}

func (l *Walog) memWrite(bufs []*buf.Buf) {
	for _, buf := range bufs {
		l.memLog = append(l.memLog, *buf)
	}
}

// Assumes caller holds memLock
// XXX absorp
func (l *Walog) doMemAppend(bufs []*buf.Buf) TxnNum {
	l.memWrite(bufs)
	txn := l.memTail + uint64(len(l.memLog))
	return TxnNum(txn)
}

func (l *Walog) readDiskHead() TxnNum {
	l.logLock.Lock()
	n := l.diskHead
	l.logLock.Unlock()
	return n
}

//
//  For clients of WAL
//

func (l *Walog) LogSz() uint64 {
	return l.logSz
}

// Scan log for blkno. If not present, read from disk
// XXX use map
func (l *Walog) Read(blkno uint64) disk.Block {
	var blk disk.Block

	l.memLock.Lock()
	if len(l.memLog) > 0 {
		for i := len(l.memLog) - 1; ; i-- {
			buf := l.memLog[i]
			if buf.Addr.Blkno == blkno {
				blk = make([]byte, disk.BlockSize)
				copy(blk, buf.Blk)
				break
			}
			if i == 0 {
				break
			}
		}
	}
	l.memLock.Unlock()
	if blk == nil {
		blk = disk.Read(blkno)
	}
	return blk
}

// Append to in-memory log. Returns false, if bufs don't fit.
// Otherwise, returns the txn for this append.
func (l *Walog) MemAppend(bufs []*buf.Buf) (TxnNum, bool) {
	l.memLock.Lock()
	if uint64(len(bufs)) > l.logSz {
		l.memLock.Unlock()
		return 0, false
	}
	l.memLock.Unlock()

	var txn TxnNum = 0
	for {
		l.memLock.Lock()
		if uint64(len(l.memLog))+uint64(len(bufs)) >= l.logSz {
			util.DPrintf(5, "memAppend: log is full; try again")
			l.memLock.Unlock()
			l.condLogger.Signal()
			l.condInstall.Signal()
			continue
		}
		txn = l.doMemAppend(bufs)
		l.memLock.Unlock()
		break
	}
	return txn, true
}

// Wait until logger has appended in-memory log through txn to on-disk
// log
func (l *Walog) LogAppendWait(txn TxnNum) {
	for {
		diskHead := l.readDiskHead()
		if txn <= diskHead {
			break
		}
		l.condLogger.Signal()
		continue
	}
}

// Wait until last started transaction has been appended to log.  If
// it is logged, then all preceeding transactions are also logged.
func (l *Walog) WaitFlushMemLog() {
	n := l.memTail + uint64(len(l.memLog))
	l.LogAppendWait(TxnNum(n))
}

// Shutdown logger and installer
func (l *Walog) Shutdown() {
	l.logLock.Lock()
	l.shutdown = true
	l.logLock.Unlock()
}
