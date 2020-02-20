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

	"github.com/mit-pdos/goose-nfsd/common"
)

const (
	HDRMETA       = uint64(8) // space for the end position
	HDRADDRS      = (disk.BlockSize - HDRMETA) / 8
	LOGSZ         = HDRADDRS
	LOGDISKBLOCKS = HDRADDRS + 2 // 2 for log header
)

const (
	LOGHDR   = common.Bnum(0)
	LOGHDR2  = common.Bnum(1)
	LOGSTART = common.Bnum(2)
)
