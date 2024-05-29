package wal

import (
	"github.com/mit-pdos/go-journal/disk"

	"github.com/mit-pdos/go-journal/common"

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
