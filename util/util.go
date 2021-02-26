package util

import (
	"log"
)

var Debug uint64 = 0

func DPrintf(level uint64, format string, a ...interface{}) {
	if level <= Debug {
		log.Printf(format, a...)
	}
}

func RoundUp(n uint64, sz uint64) uint64 {
	return (n + sz - 1) / sz
}

func Min(n uint64, m uint64) uint64 {
	if n < m {
		return n
	} else {
		return m
	}
}

// returns n+m>=2^64 (if it were computed at infinite precision)
func SumOverflows(n uint64, m uint64) bool {
	return n+m < n
}

func SumOverflows32(n uint32, m uint32) bool {
	return n+m < n
}

func CloneByteSlice(s []byte) []byte {
	s2 := make([]byte, len(s))
	copy(s2, s)
	return s2
}
