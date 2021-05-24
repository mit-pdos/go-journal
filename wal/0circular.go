package wal

import (
	"github.com/tchajed/goose/machine/disk"
	"github.com/tchajed/marshal"

	"github.com/mit-pdos/go-journal/common"
)

type LogPosition uint64
type Update = common.Update

func MkBlockData(bn common.Bnum, blk disk.Block) Update {
	b := Update{Addr: bn, Block: blk}
	return b
}

type circularAppender struct {
	diskAddrs []uint64
}

// initCircular takes ownership of the circular log, which is the first
// LOGDISKBLOCKS of the disk.
func initCircular(d disk.Disk) *circularAppender {
	b0 := make([]byte, disk.BlockSize)
	d.Write(LOGHDR, b0)
	d.Write(LOGHDR2, b0)
	addrs := make([]uint64, HDRADDRS)
	return &circularAppender{
		diskAddrs: addrs,
	}
}

// decodeHdr1 decodes (end, start) from hdr1
func decodeHdr1(hdr1 disk.Block) (uint64, []uint64) {
	dec1 := marshal.NewDec(hdr1)
	end := dec1.GetInt()
	addrs := dec1.GetInts(HDRADDRS)
	return end, addrs
}

// decodeHdr2 reads start from hdr2
func decodeHdr2(hdr2 disk.Block) uint64 {
	dec2 := marshal.NewDec(hdr2)
	start := dec2.GetInt()
	return start
}

func recoverCircular(d disk.Disk) (*circularAppender, LogPosition, LogPosition, []uint64, []disk.Block) {
	hdr1 := d.Read(LOGHDR)
	hdr2 := d.Read(LOGHDR2)
	end, addrs := decodeHdr1(hdr1)
	start := decodeHdr2(hdr2)
	var memaddrs []uint64
	var bufs []disk.Block
	for pos := start; pos < end; pos++ {
		addr := addrs[pos%LOGSZ]
		b := d.Read(LOGSTART + pos%LOGSZ)
		memaddrs = append(memaddrs, addr)
		bufs = append(bufs, b)
	}
	return &circularAppender{
		diskAddrs: addrs,
	}, LogPosition(start), LogPosition(end), memaddrs, bufs
}

func (c *circularAppender) hdr1(end LogPosition, bhdr []byte) disk.Block {
	enc := marshal.NewEncFromSlice(bhdr)
	enc.PutInt(uint64(end))
	enc.PutInts(c.diskAddrs)
	return enc.Finish()
}

func hdr2(start LogPosition) disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(start))
	return enc.Finish()
}

// Example:
// LOGSZ = 512
// base%LOGSZ = 510
// len(bufs) = 3
//
// wrapidx = 512 - 510 = 2
// Writev(LOGSTART+510, bufs[0:2]) --> Write(LOGSTART+510, bufs[0]), Write(LOGSTART+511, bufs[1])
// Writev(LOGSTART, bufs[2:]) --> Write(LOGSTART, bufs[2])


// Example 2:
// LOGSZ = 512
// base%LOGSZ = 10
// len(bufs) = 3
// wrapidx = 512 - 10 = 502
// Writev(LOGSTART+10, bufs)


func (c *circularAppender) logBlocks(d disk.Disk, end LogPosition, addrs []uint64, bufs []disk.Block) {
	base := end
	wrapidx := LOGSZ - uint64(base)%LOGSZ
	if wrapidx > uint64(len(addrs)) {
		// one writev suffices
		d.Writev(LOGSTART+uint64(base)%LOGSZ, bufs)
	} else {
		// need two writev because we wrap around
		d.Writev(LOGSTART+uint64(base)%LOGSZ, bufs[0:wrapidx])
		d.Writev(LOGSTART, bufs[wrapidx:])
	}

	for i, blkno := range addrs {
		pos := end + LogPosition(i)
		c.diskAddrs[uint64(pos)%LOGSZ] = blkno
	}
}

func (c *circularAppender) Append(d disk.Disk, end LogPosition, addrs []uint64, bufs []disk.Block, bhdr []byte) {
	c.logBlocks(d, end, addrs, bufs)
	d.Barrier()
	// atomic installation
	newEnd := end + LogPosition(len(bufs))
	b := c.hdr1(newEnd, bhdr)
	d.Write(LOGHDR, b)
	d.Barrier()
}

func Advance(d disk.Disk, newStart LogPosition) {
	b := hdr2(newStart)
	d.Write(LOGHDR2, b)
	d.Barrier()
}
