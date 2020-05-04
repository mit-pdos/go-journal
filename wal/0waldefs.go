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

	"sync"
)

type WalogState struct {
	memLog  *sliding
	diskEnd LogPosition

	// For shutdown:
	shutdown bool
	nthread  uint64
}

func (st *WalogState) memEnd() LogPosition {
	return st.memLog.end()
}

type Walog struct {
	memLock *sync.Mutex
	d       disk.Disk
	circ    *circularAppender
	st      *WalogState

	condLogger  *sync.Cond
	condInstall *sync.Cond

	// For shutdown:
	condShut *sync.Cond
}

func (l *Walog) LogSz() uint64 {
	return common.HDRADDRS
}
