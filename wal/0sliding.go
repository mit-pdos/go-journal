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

// update does an in-place absorb of an update to u
func (s *sliding) update(pos LogPosition, u Update) {
	s.log[s.mutable-s.start:][pos-s.mutable] = u
}

func (s *sliding) append(u Update) {
	pos := s.start + LogPosition(len(s.log))
	s.log = append(s.log, u)
	s.addrPos[u.Addr] = pos
}

// takeFrom takes the read-only updates from a logical start position to the
// current mutable boundary
func (s *sliding) takeFrom(start LogPosition) []Update {
	off := s.start
	return s.log[start-off : s.mutable-off]
}

// takeTill takes the read-only updates till a logical start position (which
// should be within the read-only region; that is, end <= s.mutable)
func (s *sliding) takeTill(end LogPosition) []Update {
	return s.log[:end-s.start]
}

// deleteFrom deletes read-only updates up to newStart,
// correctly updating the start position
func (s *sliding) deleteFrom(newStart LogPosition) {
	start := s.start
	for i, u := range s.log[:newStart-start] {
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

func (s *sliding) posForAddr(a common.Bnum) (LogPosition, bool) {
	pos, ok := s.addrPos[a]
	return pos, ok
}
