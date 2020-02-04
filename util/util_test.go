package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMin(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(uint64(2), Min(2, 3))
	assert.Equal(uint64(2), Min(3, 2))
	assert.Equal(uint64(2), Min(2, 2))
}

func TestRoundUp(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(uint64(4), RoundUp(10, 3))
	assert.Equal(uint64(3), RoundUp(9, 3), "exact division")
	assert.Equal(uint64(0), RoundUp(0, 3))
	assert.Equal(uint64(5), RoundUp(4096*4+4095, 4096))
	assert.Equal(uint64(5), RoundUp(4096*4+1, 4096), "round up by sz-1")
}

func TestSumOverflows(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(false, SumOverflows(1<<31, 1<<31))
	assert.Equal(false, SumOverflows(1<<64-2, 1))
	assert.Equal(false, SumOverflows(1, 1<<64-2))
	assert.Equal(false, SumOverflows(1<<32, 1<<32))

	assert.Equal(true, SumOverflows(1, 1<<64-1))
	assert.Equal(true, SumOverflows(1<<64-1, 1))
	assert.Equal(true, SumOverflows(2, 1<<64-1))
	assert.Equal(true, SumOverflows(1<<63, 1<<63))
}
