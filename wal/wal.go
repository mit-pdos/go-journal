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
const LOGSTART = uint64(1)

type Walog struct {
	// Protects in-memory-related log state
	memLock  *sync.Mutex
	memLog   []buf.Buf // in-memory log starting with memStart
	memStart LogPosition

	// Protects disk-related log state, incl. header, diskEnd,
	// shutdown
	logLock     *sync.Mutex
	condLogger  *sync.Cond
	condInstall *sync.Cond
	diskEnd     LogPosition // next block to log to disk
	shutdown    bool
}

func MkLog() *Walog {
	ll := new(sync.Mutex)
	l := &Walog{
		memLock:     new(sync.Mutex),
		logLock:     ll,
		condLogger:  sync.NewCond(ll),
		condInstall: sync.NewCond(ll),
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

// On-disk header in first block of log
type hdr struct {
	end   LogPosition
	start LogPosition
	addrs []uint64
}

func decodeHdr(blk disk.Block) *hdr {
	hdr := &hdr{
		end:   0,
		start: 0,
		addrs: nil,
	}
	dec := marshal.NewDec(blk)
	hdr.end = LogPosition(dec.GetInt())
	hdr.start = LogPosition(dec.GetInt())
	hdr.addrs = dec.GetInts(uint64(hdr.end - hdr.start))
	return hdr
}

func encodeHdr(hdr hdr, blk disk.Block) {
	enc := marshal.NewEnc(blk)
	enc.PutInt(uint64(hdr.end))
	enc.PutInt(uint64(hdr.start))
	enc.PutInts(hdr.addrs)
}

func (l *Walog) writeHdr(end LogPosition, start LogPosition, bufs []buf.Buf) {
	n := uint64(len(bufs))
	addrs := make([]uint64, n)
	if n != uint64(end-start) {
		panic("writeHdr")
	}
	for i := start; i < end; i++ {
		addrs[i-start] = bufs[i-start].Addr.Blkno
	}
	hdr := hdr{end: end, start: start, addrs: addrs}
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
	l.memStart = hdr.start
	l.diskEnd = hdr.end
	for i := uint64(0); i < uint64(hdr.end-hdr.start); i++ {
		util.DPrintf(1, "recover block %d\n", hdr.addrs[i])
		blk := disk.Read(LOGSTART + i)
		a := buf.MkAddr(hdr.addrs[i], 0, fs.NBITBLOCK)
		b := buf.MkBuf(a, blk)
		l.memLog = append(l.memLog, *b)
	}
}

func (l *Walog) memWrite(bufs []*buf.Buf) {
	for _, buf := range bufs {
		l.memLog = append(l.memLog, *buf)
	}
}

// Assumes caller holds memLock
// XXX absorp
func (l *Walog) doMemAppend(bufs []*buf.Buf) LogPosition {
	l.memWrite(bufs)
	txn := l.memStart + LogPosition(len(l.memLog))
	return txn
}

func (l *Walog) readDiskEnd() LogPosition {
	l.logLock.Lock()
	n := l.diskEnd
	l.logLock.Unlock()
	return n
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
	l.memLock.Lock()
	if uint64(len(bufs)) > l.LogSz() {
		l.memLock.Unlock()
		return 0, false
	}
	l.memLock.Unlock()

	var txn LogPosition = 0
	for {
		l.memLock.Lock()
		if uint64(len(l.memLog))+uint64(len(bufs)) >= l.LogSz() {
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
func (l *Walog) LogAppendWait(txn LogPosition) {
	for {
		diskEnd := l.readDiskEnd()
		if txn <= diskEnd {
			break
		}
		l.condLogger.Signal()
		continue
	}
}

// Wait until last started transaction has been appended to log.  If
// it is logged, then all preceeding transactions are also logged.
func (l *Walog) WaitFlushMemLog() {
	n := l.memStart + LogPosition(len(l.memLog))
	l.LogAppendWait(n)
}

// Shutdown logger and installer
func (l *Walog) Shutdown() {
	l.logLock.Lock()
	l.shutdown = true
	l.logLock.Unlock()
}
