package buf

import (
	"github.com/tchajed/goose/machine/disk"

	"fmt"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInstallBits(t *testing.T) {
	r := installBits(byte(0x1F), byte(0x0), 4, 1)
	assert.Equal(t, byte(0x10), r)
	r = installBits(byte(0xF), byte(0x1F), 4, 1)
	assert.Equal(t, byte(0x0F), r)
	r = installBits(byte(0xFF), byte(0x0), 4, 2)
	assert.Equal(t, byte(0x30), r)
}

func TestCopyBits(t *testing.T) {
	dst := []byte{0x0}
	copyBits([]byte{0x1F}, dst, 4, 1)
	assert.Equal(t, byte(0x10), dst[0])

	dst = []byte{0x0, 0x0}
	copyBits([]byte{0xFF, 0xFF}, dst, 4, 8)
	assert.Equal(t, byte(0xF0), dst[0])
	assert.Equal(t, byte(0x0F), dst[1])

	dst = []byte{0x0, 0x0, 0x0}
	copyBits([]byte{0xFF, 0xFF}, dst, 4, 16)
	assert.Equal(t, byte(0xF0), dst[0])
	assert.Equal(t, byte(0xFF), dst[1])
	assert.Equal(t, byte(0x0F), dst[2])
}

func TestCopyAligned(t *testing.T) {
	dst := make([]byte, disk.BlockSize)
	src := make([]byte, 16)
	src[0] = byte(0xFF)
	src[1] = byte(0xF0)
	src[2] = byte(0x03)
	dst[3] = byte(0xF0)
	copyBitsAligned(src, dst, 8, 16)
	fmt.Printf("dst %v\n", dst[0:4])
	assert.Equal(t, byte(0xFF), dst[1])
	assert.Equal(t, byte(0xF0), dst[2])
	assert.Equal(t, byte(0xF0), dst[3])
	copyBitsAligned(src, dst, 8, 18)
	fmt.Printf("dst %v\n", dst[0:4])
	assert.Equal(t, byte(0xFF), dst[1])
	assert.Equal(t, byte(0xF0), dst[2])
	assert.Equal(t, byte(0xF3), dst[3])
}
