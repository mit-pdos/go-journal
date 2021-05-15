package wal

import (
	"github.com/tchajed/goose/machine/disk"
	"github.com/tchajed/marshal"

	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/util"
)

type LogPosition uint64

type Update struct {
	Addr  common.Bnum
	Block disk.Block
}

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

func recoverCircular(d disk.Disk) (*circularAppender, LogPosition, LogPosition, []Update) {
	hdr1 := d.Read(LOGHDR)
	hdr2 := d.Read(LOGHDR2)
	end, addrs := decodeHdr1(hdr1)
	start := decodeHdr2(hdr2)
	var bufs []Update
	for pos := start; pos < end; pos++ {
		addr := addrs[pos%LOGSZ]
		b := d.Read(LOGSTART + pos%LOGSZ)
		bufs = append(bufs, Update{Addr: addr, Block: b})
	}
	return &circularAppender{
		diskAddrs: addrs,
	}, LogPosition(start), LogPosition(end), bufs
}

func (c *circularAppender) hdr1(end LogPosition) disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(end))
	enc.PutInts(c.diskAddrs)
	return enc.Finish()
}

func hdr2(start LogPosition) disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(start))
	return enc.Finish()
}

func (c *circularAppender) logBlocks(d disk.Disk, end LogPosition, bufs []Update) {
	for i, buf := range bufs {
		pos := end + LogPosition(i)
		blk := buf.Block
		blkno := buf.Addr
		util.DPrintf(5,
			"logBlocks: %d to log block %d\n", blkno, pos)
		d.Write(LOGSTART+uint64(pos)%LOGSZ, blk)
		c.diskAddrs[uint64(pos)%LOGSZ] = blkno
	}
}

func (c *circularAppender) Append(d disk.Disk, end LogPosition, bufs []Update) {
	c.logBlocks(d, end, bufs)
	d.Barrier()
	// atomic installation
	newEnd := end + LogPosition(len(bufs))
	b := c.hdr1(newEnd)
	d.Write(LOGHDR, b)
	d.Barrier()
}

func Advance(d disk.Disk, newStart LogPosition) {
	b := hdr2(newStart)
	d.Write(LOGHDR2, b)
	d.Barrier()
}
