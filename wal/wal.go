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
	memHead   uint64    // head of in-memory log
	memTail   uint64    // tail of in-memory log
	txnNxt    TxnNum    // next transaction number
	dsktxnNxt TxnNum    // next transaction number to install

	// Protects disk-related log state, incl. header, logtxnNxt,
	// shutdown
	logLock     *sync.Mutex
	condLogger  *sync.Cond
	condInstall *sync.Cond
	logtxnNxt   TxnNum // next transaction number to log
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
		memHead:     0,
		memTail:     0,
		txnNxt:      0,
		logtxnNxt:   0,
		dsktxnNxt:   0,
		shutdown:    false,
	}
	util.DPrintf(1, "mkLog: size %d\n", l.logSz)
	l.writeHdr(0, 0, 0, l.memLog)

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
	logTxnNxt TxnNum // next txn to log
	addrs     []uint64
}

func decodeHdr(blk disk.Block) *hdr {
	hdr := &hdr{
		head:      0,
		tail:      0,
		logTxnNxt: 0,
		addrs:     nil,
	}
	dec := marshal.NewDec(blk)
	hdr.head = dec.GetInt()
	hdr.tail = dec.GetInt()
	hdr.logTxnNxt = TxnNum(dec.GetInt())
	hdr.addrs = dec.GetInts(hdr.head - hdr.tail)
	return hdr
}

func encodeHdr(hdr hdr, blk disk.Block) {
	enc := marshal.NewEnc(blk)
	enc.PutInt(hdr.head)
	enc.PutInt(hdr.tail)
	enc.PutInt(uint64(hdr.logTxnNxt))
	enc.PutInts(hdr.addrs)
}

func (l *Walog) index(index uint64) uint64 {
	return index - l.memTail
}

func (l *Walog) writeHdr(head uint64, tail uint64, dsktxnnxt TxnNum, bufs []buf.Buf) {
	n := uint64(len(bufs))
	addrs := make([]uint64, n)
	if n != head-tail {
		panic("writeHdr")
	}
	for i := tail; i < head; i++ {
		addrs[l.index(i)] = bufs[l.index(i)].Addr.Blkno
	}
	hdr := hdr{head: head, tail: tail, logTxnNxt: dsktxnnxt, addrs: addrs}
	blk := make(disk.Block, disk.BlockSize)
	encodeHdr(hdr, blk)
	disk.Write(LOGHDR, blk)
}

func (l *Walog) readHdr() *hdr {
	blk := disk.Read(LOGHDR)
	hdr := decodeHdr(blk)
	return hdr
}

func (l *Walog) memWrite(bufs []*buf.Buf) {
	for _, buf := range bufs {
		l.memLog = append(l.memLog, *buf)
	}
	l.memHead = l.memHead + uint64(len(bufs))
}

// Assumes caller holds memLock
// XXX absorp
func (l *Walog) doMemAppend(bufs []*buf.Buf) TxnNum {
	l.memWrite(bufs)
	txn := l.txnNxt
	l.txnNxt = l.txnNxt + 1
	return txn
}

func (l *Walog) readLogTxnNxt() TxnNum {
	l.logLock.Lock()
	n := l.logtxnNxt
	l.logLock.Unlock()
	return n
}

func (l *Walog) readtxnNxt() TxnNum {
	l.memLock.Lock()
	n := l.txnNxt
	l.memLock.Unlock()
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
	if l.memHead > l.memTail {
		for i := l.memHead - 1; ; i-- {
			buf := l.memLog[l.index(i)]
			if buf.Addr.Blkno == blkno {
				blk = make([]byte, disk.BlockSize)
				copy(blk, buf.Blk)
				break
			}
			if i <= l.memTail {
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
		if l.index(l.memHead)+uint64(len(bufs)) >= l.logSz {
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
		logtxn := l.readLogTxnNxt()
		if txn < logtxn {
			break
		}
		l.condLogger.Signal()
		continue
	}
}

// Wait until last started transaction has been appended to log.  If
// it is logged, then all preceeding transactions are also logged.
func (l *Walog) WaitFlushMemLog() {
	n := l.readtxnNxt() - 1
	l.LogAppendWait(n)
}

//
// Logger
//

func (l *Walog) logBlocks(memhead uint64, diskhead uint64, bufs []buf.Buf) {
	for i := diskhead; i < memhead; i++ {
		bindex := i - diskhead
		blk := bufs[bindex].Blk
		blkno := bufs[bindex].Addr.Blkno
		util.DPrintf(5, "logBlocks: %d to log block %d\n", blkno, l.index(i))
		disk.Write(LOGSTART+l.index(i), blk)
	}
}

// Logger holds logLock
func (l *Walog) logAppend() {
	hdr := l.readHdr()
	l.memLock.Lock()
	memhead := l.memHead
	memtail := l.memTail
	memlog := l.memLog
	txnnxt := l.txnNxt
	if memtail != hdr.tail || memhead < hdr.head {
		panic("logAppend")
	}
	l.memLock.Unlock()

	//util.DPrintf("logAppend memhead %d memtail %d diskhead %d disktail %d txnnxt %d\n", memhead, memtail, hdr.head, hdr.tail, txnnxt)
	newbufs := memlog[l.index(hdr.head):l.index(memhead)]
	l.logBlocks(memhead, hdr.head, newbufs)
	l.writeHdr(memhead, memtail, txnnxt, memlog)

	l.logtxnNxt = txnnxt
}

func (l *Walog) logger() {
	l.logLock.Lock()
	for !l.shutdown {
		l.logAppend()
		l.condLogger.Wait()
	}
	l.logLock.Unlock()
}

//
// Installer
//

// Install blocks in on-disk log to their home location.
func (l *Walog) installer() {
	l.logLock.Lock()
	for !l.shutdown {
		blknos, txn := l.logInstall()
		if len(blknos) > 0 {
			util.DPrintf(5, "Installed till txn %d\n", txn)
		}
		l.condInstall.Wait()
	}
	l.logLock.Unlock()
}

func (l *Walog) installBlocks(bufs []buf.Buf) {
	n := uint64(len(bufs))
	for i := uint64(0); i < n; i++ {
		blkno := bufs[i].Addr.Blkno
		blk := bufs[i].Blk
		util.DPrintf(5, "installBlocks: write log block %d to %d\n", i, blkno)
		disk.Write(blkno, blk)
	}
}

// Installer holds logLock
// XXX absorp
func (l *Walog) logInstall() ([]uint64, TxnNum) {
	hdr := l.readHdr()
	bufs := l.memLog[l.index(hdr.tail):l.index(hdr.head)]
	util.DPrintf(1, "logInstall diskhead %d disktail %d\n", hdr.head, hdr.tail)
	l.installBlocks(bufs)
	hdr.tail = hdr.head
	l.writeHdr(hdr.head, hdr.tail, hdr.logTxnNxt, nil)
	l.memLock.Lock()

	if hdr.tail < l.memTail {
		panic("logInstall")
	}
	l.memLog = l.memLog[l.index(hdr.tail):l.index(l.memHead)]
	l.memTail = hdr.tail
	l.dsktxnNxt = hdr.logTxnNxt
	l.memLock.Unlock()
	return hdr.addrs, hdr.logTxnNxt
}

// Shutdown logger and installer
func (l *Walog) Shutdown() {
	l.logLock.Lock()
	l.shutdown = true
	l.logLock.Unlock()
}
