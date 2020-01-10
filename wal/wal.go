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

type LogPosition uint64

const LOGHDR = uint64(0)
const LOGHDR2 = uint64(1)
const LOGSTART = uint64(2)

type Walog struct {
	memLock *sync.Mutex

	condLogger  *sync.Cond
	condInstall *sync.Cond

	memLog   []buf.Buf // in-memory log starting with memStart
	memStart LogPosition
	diskEnd  LogPosition // next block to log to disk
	shutdown bool
}

func MkLog() *Walog {
	ml := new(sync.Mutex)
	l := &Walog{
		memLock:     ml,
		condLogger:  sync.NewCond(ml),
		condInstall: sync.NewCond(ml),
		memLog:      make([]buf.Buf, 0),
		memStart:    0,
		diskEnd:     0,
		shutdown:    false,
	}
	util.DPrintf(1, "mkLog: size %d\n", l.LogSz())

	l.recover()

	// TODO: do we still need to use machine.Spawn,
	//  or can we just use go statements?
	machine.Spawn(func() { l.logger() })
	machine.Spawn(func() { l.installer() })

	return l
}

// On-disk header in the first block of the log
type hdr struct {
	end   LogPosition
	addrs []uint64
}

func decodeHdr(blk disk.Block) *hdr {
	h := &hdr{
		end:   0,
		addrs: nil,
	}
	dec := marshal.NewDec(blk)
	h.end = LogPosition(dec.GetInt())
	h.addrs = dec.GetInts(fs.HDRADDRS)
	return h
}

func encodeHdr(h hdr, blk disk.Block) {
	enc := marshal.NewEnc(blk)
	enc.PutInt(uint64(h.end))
	enc.PutInts(h.addrs)
}

// On-disk header in the second block of the log
type hdr2 struct {
	start LogPosition
}

func decodeHdr2(blk disk.Block) *hdr2 {
	h := &hdr2{
		start: 0,
	}
	dec := marshal.NewDec(blk)
	h.start = LogPosition(dec.GetInt())
	return h
}

func encodeHdr2(h hdr2, blk disk.Block) {
	enc := marshal.NewEnc(blk)
	enc.PutInt(uint64(h.start))
}

func (l *Walog) writeHdr(h *hdr) {
	blk := make(disk.Block, disk.BlockSize)
	encodeHdr(*h, blk)
	disk.Write(LOGHDR, blk)
}

func (l *Walog) readHdr() *hdr {
	blk := disk.Read(LOGHDR)
	h := decodeHdr(blk)
	return h
}

func (l *Walog) writeHdr2(h *hdr2) {
	blk := make(disk.Block, disk.BlockSize)
	encodeHdr2(*h, blk)
	disk.Write(LOGHDR2, blk)
}

func (l *Walog) readHdr2() *hdr2 {
	blk := disk.Read(LOGHDR2)
	h := decodeHdr2(blk)
	return h
}

func (l *Walog) recover() {
	h := l.readHdr()
	h2 := l.readHdr2()
	l.memStart = h2.start
	l.diskEnd = h.end
	for pos := h2.start; pos < h.end; pos++ {
		addr := h.addrs[uint64(pos)%l.LogSz()]
		util.DPrintf(1, "recover block %d\n", addr)
		blk := disk.Read(LOGSTART + (uint64(pos) % l.LogSz()))
		a := buf.MkAddr(addr, 0, fs.NBITBLOCK)
		b := buf.MkBuf(a, blk)
		l.memLog = append(l.memLog, *b)
	}
}

func (l *Walog) memWrite(bufs []*buf.Buf) {
	for _, buf := range bufs {
		l.memLog = append(l.memLog, *buf)
	}
	l.condLogger.Broadcast()
}

// Assumes caller holds memLock
// XXX absorp
func (l *Walog) doMemAppend(bufs []*buf.Buf) LogPosition {
	l.memWrite(bufs)
	txn := l.memStart + LogPosition(len(l.memLog))
	return txn
}

//
//  For clients of WAL
//

func (l *Walog) LogSz() uint64 {
	return fs.HDRADDRS
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
func (l *Walog) MemAppend(bufs []*buf.Buf) (LogPosition, bool) {
	if uint64(len(bufs)) > l.LogSz() {
		return 0, false
	}

	var txn LogPosition = 0
	for {
		l.memLock.Lock()
		if uint64(l.memStart)+uint64(len(l.memLog))-uint64(l.diskEnd)+uint64(len(bufs)) > l.LogSz() {
			util.DPrintf(5, "memAppend: log is full; try again")
			l.memLock.Unlock()
			l.condLogger.Broadcast()
			l.condInstall.Broadcast()
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
func (l *Walog) LogAppendWait(txn LogPosition) {
	l.memLock.Lock()
	for {
		if txn <= l.diskEnd {
			break
		}
		l.condLogger.Wait()
	}
	l.memLock.Unlock()
}

// Wait until last started transaction has been appended to log.  If
// it is logged, then all preceeding transactions are also logged.
func (l *Walog) WaitFlushMemLog() {
	l.memLock.Lock()
	n := l.memStart + LogPosition(len(l.memLog))
	l.memLock.Unlock()

	l.LogAppendWait(n)
}

// Shutdown logger and installer
func (l *Walog) Shutdown() {
	l.memLock.Lock()
	l.shutdown = true
	l.condLogger.Broadcast()
	l.condInstall.Broadcast()
	l.memLock.Unlock()
}
