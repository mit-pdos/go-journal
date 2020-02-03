package common

import (
	"github.com/tchajed/goose/machine/disk"
)

const (
	NBITBLOCK    uint64 = disk.BlockSize * 8
	INODEBLK     uint64 = disk.BlockSize / INODESZ
	NINODEBITMAP uint64 = 1

	INODESZ uint64 = 128 // on-disk size

	HDRMETA  = uint64(8) // space for the end position
	HDRADDRS = (disk.BlockSize - HDRMETA) / 8
	LOGSIZE  = HDRADDRS + 2 // 2 for log header
)

type Inum uint64
type Bnum = uint64

const (
	NULLINUM Inum = 0
	ROOTINUM Inum = 1
	NULLBNUM Bnum = 0
)
