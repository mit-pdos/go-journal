package wal

import (
	"github.com/mit-pdos/goose-nfsd/common"
	"github.com/mit-pdos/goose-nfsd/util"
)

type sliding struct {
	log     []Update
	start   LogPosition
	mutable LogPosition
	addrPos map[common.Bnum]LogPosition
}

func mkSliding(log []Update, start LogPosition) *sliding {
	addrPos := make(map[common.Bnum]LogPosition)
	for i, buf := range log {
		addrPos[buf.Addr] = start + LogPosition(i)
	}
	return &sliding{
		log:     log,
		start:   start,
		mutable: start + LogPosition(len(log)),
		addrPos: addrPos,
	}
}

func (s *sliding) end() LogPosition {
	return s.start + LogPosition(len(s.log))
}

func (s *sliding) get(pos LogPosition) Update {
	return s.log[pos-s.start]
}

func (s *sliding) posForAddr(a common.Bnum) (LogPosition, bool) {
	pos, ok := s.addrPos[a]
	return pos, ok
}

// update does an in-place absorb of an update to u
//
// internal to sliding
func (s *sliding) update(pos LogPosition, u Update) {
	s.log[s.mutable-s.start:][pos-s.mutable] = u
}

// append writes an update that cannot be absorbed
//
// internal to sliding
func (s *sliding) append(u Update) {
	pos := s.start + LogPosition(len(s.log))
	s.log = append(s.log, u)
	s.addrPos[u.Addr] = pos
}

// Absorbs writes in in-memory transactions (avoiding those that might be in
// the process of being logged or installed).
//
// Assumes caller holds memLock
func (s *sliding) memWrite(bufs []Update) {
	// pos is only for debugging
	var pos = s.end()
	for _, buf := range bufs {
		// remember most recent position for Blkno
		oldpos, ok := s.posForAddr(buf.Addr)
		if ok && oldpos >= s.mutable {
			util.DPrintf(5, "memWrite: absorb %d pos %d old %d\n",
				buf.Addr, pos, oldpos)
			s.update(oldpos, buf)
		} else {
			if ok {
				util.DPrintf(5, "memLogMap: replace %d pos %d old %d\n",
					buf.Addr, pos, oldpos)
			} else {
				util.DPrintf(5, "memLogMap: add %d pos %d\n",
					buf.Addr, pos)
			}
			s.append(buf)
			pos += 1
		}
	}
}

// takeFrom takes the read-only updates from a logical start position to the
// current mutable boundary
func (s *sliding) takeFrom(start LogPosition) []Update {
	return s.log[:s.mutable-s.start][start-s.start:]
}

// takeTill takes the read-only updates till a logical start position (which
// should be within the read-only region; that is, end <= s.mutable)
func (s *sliding) takeTill(end LogPosition) []Update {
	return s.log[:s.mutable-s.start][:end-s.start]
}

func (s *sliding) intoMutable() []Update {
	return s.log[s.mutable-s.start:]
}

// deleteFrom deletes read-only updates up to newStart,
// correctly updating the start position
func (s *sliding) deleteFrom(newStart LogPosition) {
	start := s.start
	for i, u := range s.log[:s.mutable-start][:newStart-start] {
		pos := start + LogPosition(i)
		blkno := u.Addr
		oldPos, ok := s.addrPos[blkno]
		if ok && oldPos <= pos {
			util.DPrintf(5, "memLogMap: del %d %d\n", blkno, oldPos)
			delete(s.addrPos, blkno)
		}
	}
	s.log = s.log[newStart-start:]
	s.start = newStart
}

func (s *sliding) clearMutable() {
	s.mutable = s.end()
}
