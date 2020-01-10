package util

import "log"

const Debug uint64 = 1

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
