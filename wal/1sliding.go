package wal

type sliding struct {
	log     []Update
	start   LogPosition
	mutable LogPosition
}

func (s *sliding) end() LogPosition {
	return s.start + LogPosition(len(s.log))
}

func (s *sliding) get(pos LogPosition) Update {
	return s.log[pos-s.start]
}

// update does an in-place absorb of an update to u
func (s *sliding) update(pos LogPosition, u Update) {
	s.log[pos-s.start] = u
}

func (s *sliding) append(u Update) {
	s.log = append(s.log, u)
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
	s.log = s.log[newStart-s.start:]
	s.start = newStart
}

func (s *sliding) clearMutable() {
	s.mutable = s.end()
}
