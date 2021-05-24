package wal

import (
	"github.com/tchajed/goose/machine/disk"
	"github.com/mit-pdos/go-journal/common"
	"github.com/mit-pdos/go-journal/util"
)

type sliding struct {
	addrlog     []uint64
	blocklog    []disk.Block
	start   LogPosition
	mutable LogPosition
	addrPos map[common.Bnum]LogPosition
}

func mkSliding(addrlog []uint64, blocklog []disk.Block, start LogPosition) *sliding {
	addrPos := make(map[common.Bnum]LogPosition)
	for i, addr := range addrlog {
		addrPos[addr] = start + LogPosition(i)
	}
	return &sliding{
		addrlog:  addrlog,
		blocklog: blocklog,
		start:   start,
		mutable: start + LogPosition(len(addrlog)),
		addrPos: addrPos,
	}
}

func (s *sliding) end() LogPosition {
	return s.start + LogPosition(len(s.addrlog))
}

/*
func (s *sliding) get(pos LogPosition) Update {
	return s.log[pos-s.start]
}
*/

func (s *sliding) posForAddr(a common.Bnum) (LogPosition, bool) {
	pos, ok := s.addrPos[a]
	return pos, ok
}

// update does an in-place absorb of an update to u
//
// internal to sliding
func (s *sliding) update(pos LogPosition, blk disk.Block) {
	s.blocklog[s.mutable-s.start:][pos-s.mutable] = blk
}

// append writes an update that cannot be absorbed
//
// internal to sliding
func (s *sliding) append(addr uint64, blk disk.Block) {
	pos := s.start + LogPosition(len(s.addrlog))
	s.addrlog = append(s.addrlog, addr)
	s.blocklog = append(s.blocklog, blk)
	s.addrPos[addr] = pos
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
			s.update(oldpos, buf.Block)
		} else {
			if ok {
				util.DPrintf(5, "memLogMap: replace %d pos %d old %d\n",
					buf.Addr, pos, oldpos)
			} else {
				util.DPrintf(5, "memLogMap: add %d pos %d\n",
					buf.Addr, pos)
			}
			s.append(buf.Addr, buf.Block)
			pos += 1
		}
	}
}

/* FIXME */
func (s *sliding) memWrite2(addrs []uint64, bufs []disk.Block) {
	// pos is only for debugging
	var pos = s.end()
	for i, addr := range addrs {
		// remember most recent position for Blkno
		oldpos, ok := s.posForAddr(addr)
		if ok && oldpos >= s.mutable {
			util.DPrintf(5, "memWrite: absorb %d pos %d old %d\n",
				addr, pos, oldpos)
			s.update(oldpos, bufs[i])
		} else {
			if ok {
				util.DPrintf(5, "memLogMap: replace %d pos %d old %d\n",
					addr, pos, oldpos)
			} else {
				util.DPrintf(5, "memLogMap: add %d pos %d\n",
					addr, pos)
			}
			s.append(addr, bufs[i])
			pos += 1
		}
	}
}

// takeFrom takes the read-only updates from a logical start position to the
// current mutable boundary
func (s *sliding) takeFrom(start LogPosition) ([]uint64, []disk.Block) {
	return s.addrlog[:s.mutable-s.start][start-s.start:], s.blocklog[:s.mutable-s.start][start-s.start:]
}

// takeTill takes the read-only updates till a logical start position (which
// should be within the read-only region; that is, end <= s.mutable)
func (s *sliding) takeTill(end LogPosition) ([]uint64, []disk.Block) {
	return s.addrlog[:s.mutable-s.start][:end-s.start], s.blocklog[:s.mutable-s.start][:end-s.start]
}

func (s *sliding) intoMutable() ([]uint64, []disk.Block) {
	return s.addrlog[s.mutable-s.start:], s.blocklog[s.mutable-s.start:]
}

// deleteFrom deletes read-only updates up to newStart,
// correctly updating the start position
func (s *sliding) deleteFrom(newStart LogPosition) {
	start := s.start
	for i, blkno := range s.addrlog[:s.mutable-start][:newStart-start] {
		pos := start + LogPosition(i)
		oldPos, ok := s.addrPos[blkno]
		if ok && oldPos <= pos {
			util.DPrintf(5, "memLogMap: del %d %d\n", blkno, oldPos)
			delete(s.addrPos, blkno)
		}
	}
	s.addrlog = s.addrlog[newStart-start:]
	s.blocklog = s.blocklog[newStart-start:]
	s.start = newStart
}

func (s *sliding) clearMutable() {
	s.mutable = s.end()
}
