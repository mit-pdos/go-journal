//  wal implements write-ahead logging
//
//  The layout of log:
//  [ installed writes | logged writes | in-memory/logged | unstable in-memory ]
//   ^                   ^               ^                  ^
//   0                   memStart        diskEnd            nextDiskEnd
//
//  Blocks in the range [diskEnd, nextDiskEnd) are in the process of
//  being logged.  Blocks in unstable are unstably committed (i.e.,
//  written by NFS Write with the unstable flag and they can be lost
//  on crash). Later transactions may absorb them (e.g., a later NFS
//  write may update the same inode or indirect block).  The code
//  implements a policy of postponing writing unstable blocks to disk
//  as long as possible to maximize the chance of absorption (i.e.,
//  commitWait or log is full).  It may better to start logging
//  earlier.
package wal

import (
	"github.com/tchajed/goose/machine/disk"
	"github.com/tchajed/marshal"

	"github.com/mit-pdos/goose-nfsd/buf"
	"github.com/mit-pdos/goose-nfsd/fake-bcache/bcache"

	"sync"
)

const (
	HDRMETA       = uint64(8) // space for the end position
	HDRADDRS      = (disk.BlockSize - HDRMETA) / 8
	LOGSZ         = HDRADDRS
	LOGDISKBLOCKS = HDRADDRS + 2 // 2 for log header
)

type LogPosition uint64

const (
	LOGHDR   = buf.Bnum(0)
	LOGHDR2  = buf.Bnum(1)
	LOGSTART = buf.Bnum(2)
)

type BlockData struct {
	bn  buf.Bnum
	blk disk.Block
}

func MkBlockData(bn buf.Bnum, blk disk.Block) BlockData {
	b := BlockData{bn: bn, blk: blk}
	return b
}

type Walog struct {
	memLock *sync.Mutex
	d       *bcache.Bcache

	condLogger  *sync.Cond
	condInstall *sync.Cond

	memLog      []BlockData // in-memory log starting with memStart
	memStart    LogPosition
	diskEnd     LogPosition // next block to log to disk
	nextDiskEnd LogPosition

	// For shutdown:
	shutdown bool
	nthread  uint64
	condShut *sync.Cond

	// For speeding up reads:
	memLogMap map[buf.Bnum]LogPosition
}

// On-disk header in the first block of the log
type hdr struct {
	end   LogPosition
	addrs []buf.Bnum
}

func decodeHdr(blk disk.Block) *hdr {
	h := &hdr{
		end:   0,
		addrs: nil,
	}
	dec := marshal.NewDec(blk)
	h.end = LogPosition(dec.GetInt())
	h.addrs = dec.GetInts(HDRADDRS)
	return h
}

func encodeHdr(h hdr) disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(h.end))
	enc.PutInts(h.addrs)
	return enc.Finish()
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

func encodeHdr2(h hdr2) disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(h.start))
	return enc.Finish()
}

func (l *Walog) writeHdr(h *hdr) {
	blk := encodeHdr(*h)
	l.d.Write(uint64(LOGHDR), blk)
}

func (l *Walog) readHdr() *hdr {
	blk := l.d.Read(uint64(LOGHDR))
	h := decodeHdr(blk)
	return h
}

func (l *Walog) writeHdr2(h *hdr2) {
	blk := encodeHdr2(*h)
	l.d.Write(uint64(LOGHDR2), blk)
}

func (l *Walog) readHdr2() *hdr2 {
	blk := l.d.Read(uint64(LOGHDR2))
	h := decodeHdr2(blk)
	return h
}

func posToDiskAddr(pos LogPosition) uint64 {
	return LOGSTART + uint64(pos)%LOGSZ
}
