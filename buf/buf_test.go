package buf

import (
	"github.com/tchajed/goose/machine/disk"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInstallOneBit(t *testing.T) {
	r := installOneBit(byte(0x1F), byte(0x0), 4)
	assert.Equal(t, byte(0x10), r)
	r = installOneBit(byte(0xF), byte(0x1F), 4)
	assert.Equal(t, byte(0x0F), r)
}

func TestInstallBit(t *testing.T) {
	dst := []byte{0x0}
	installBit([]byte{0x1F}, dst, 4)
	assert.Equal(t, byte(0x10), dst[0])
}

func TestInstallBytes(t *testing.T) {
	dst := make([]byte, disk.BlockSize)
	src := make([]byte, 16)
	src[0] = byte(0xFF)
	src[1] = byte(0xF0)
	src[2] = byte(0x03)
	dst[3] = byte(0xF0)
	installBytes(src, dst, 8, 16)
	assert.Equal(t, byte(0xFF), dst[1])
	assert.Equal(t, byte(0xF0), dst[2])
	assert.Equal(t, byte(0xF0), dst[3])
}
