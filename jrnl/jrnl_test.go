package jrnl

import (
	"testing"

	"github.com/mit-pdos/go-journal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/tchajed/goose/machine/disk"
)

func TestSizeConstants(t *testing.T) {
	assert.Equal(t, wal.LOGSZ, LogBlocks)
	assert.Equal(t, disk.BlockSize*wal.LOGSZ, LogBytes)
}
