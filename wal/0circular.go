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

type circular struct {
	diskAddrs []uint64
}

// initCircular takes ownership of the circular log, which is the first
// LOGDISKBLOCKS of the disk.
func initCircular(d disk.Disk) *circular {
	b0 := make([]byte, disk.BlockSize)
	d.Write(LOGHDR, b0)
	d.Write(LOGHDR2, b0)
	addrs := make([]uint64, HDRADDRS)
	return &circular{
		diskAddrs: addrs,
	}
}

func recoverCircular(d disk.Disk) (*circular, LogPosition, LogPosition, []Update) {
	hdr1 := d.Read(LOGHDR)
	dec1 := marshal.NewDec(hdr1)
	end := dec1.GetInt()
	addrs := dec1.GetInts(HDRADDRS)
	hdr2 := d.Read(LOGHDR2)
	dec2 := marshal.NewDec(hdr2)
	start := dec2.GetInt()
	var bufs []Update
	for pos := start; pos < end; pos++ {
		addr := addrs[pos%LOGSZ]
		b := d.Read(LOGSTART + pos%LOGSZ)
		bufs = append(bufs, Update{Addr: addr, Block: b})
	}
	return &circular{
		diskAddrs: addrs,
	}, LogPosition(start), LogPosition(end), bufs
}

func (c *circular) hdr1(end LogPosition) disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(end))
	enc.PutInts(c.diskAddrs)
	return enc.Finish()
}

func (c *circular) hdr2(start LogPosition) disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(start))
	return enc.Finish()
}

func (c *circular) logBlocks(d disk.Disk, end LogPosition, bufs []Update) {
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

func (c *circular) Append(d disk.Disk, end LogPosition, bufs []Update) {
	c.logBlocks(d, end, bufs)
	// atomic installation
	newEnd := end + LogPosition(len(bufs))
	b := c.hdr1(newEnd)
	d.Write(LOGHDR, b)
	d.Barrier()
}

func (c *circular) Advance(d disk.Disk, newStart LogPosition) {
	b := c.hdr2(newStart)
	d.Write(LOGHDR2, b)
	d.Barrier()
}
