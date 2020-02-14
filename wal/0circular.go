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
	d         disk.Disk
	diskStart LogPosition
	diskEnd   LogPosition
	diskAddrs []uint64
}

// initCircular takes ownership of the circular log, which is the first
// LOGDISKBLOCKS of the disk.
func initCircular(d disk.Disk) (*circular, []Update) {
	b0 := make([]byte, disk.BlockSize)
	d.Write(LOGHDR, b0)
	d.Write(LOGHDR2, b0)
	addrs := make([]uint64, HDRADDRS)
	return &circular{
		d:         d,
		diskStart: 0,
		diskEnd:   0,
		diskAddrs: addrs,
	}, nil
}

func recoverCircular(d disk.Disk) (*circular, []Update) {
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
		d:         d,
		diskStart: LogPosition(start),
		diskEnd:   LogPosition(end),
		diskAddrs: addrs,
	}, bufs
}

func (c *circular) SpaceRemaining() uint64 {
	return LOGSZ - uint64(c.diskEnd-c.diskStart)
}

func (c *circular) hdr1() disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(c.diskEnd))
	enc.PutInts(c.diskAddrs)
	return enc.Finish()
}

func (c *circular) hdr2() disk.Block {
	enc := marshal.NewEnc(disk.BlockSize)
	enc.PutInt(uint64(c.diskStart))
	return enc.Finish()
}

func (c *circular) appendFreeSpace(bufs []Update) {
	if c.SpaceRemaining() < uint64(len(bufs)) {
		panic("append would overflow circular log")
	}
	for i, buf := range bufs {
		pos := c.diskEnd + LogPosition(i)
		blk := buf.Block
		blkno := buf.Addr
		util.DPrintf(5,
			"logBlocks: %d to log block %d\n", blkno, pos)
		c.d.Write(LOGSTART+uint64(pos)%LOGSZ, blk)
		c.diskAddrs[uint64(pos)%LOGSZ] = blkno
	}
	c.diskEnd = c.diskEnd + LogPosition(len(bufs))
}

func (c *circular) Append(bufs []Update) {
	c.appendFreeSpace(bufs)
	// atomic installation
	b := c.hdr1()
	c.d.Write(LOGHDR, b)
	c.d.Barrier()
}

func (c *circular) Empty() {
	c.diskStart = c.diskEnd
	b := c.hdr2()
	c.d.Write(LOGHDR2, b)
	c.d.Barrier()
}
