package disk

import (
	"fmt"
	"sync"

	"golang.org/x/sys/unix"
)

var _ Disk = (*fileDisk)(nil)

type fileDisk struct {
	fd        int
	numBlocks uint64
}

func num2byte(num uint64) uint64 {
	return num
	// bs := make([]byte, 4)
	// binary.LittleEndian.PutUint32(bs, uint32(num))
	// // fmt.Println(bs)
	// return bs
}

func NewFileDisk(path string, numBlocks uint64) (fileDisk, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT, 0666)
	if err != nil {
		return fileDisk{}, err
	}
	var stat unix.Stat_t
	err = unix.Fstat(fd, &stat)
	if err != nil {
		return fileDisk{}, err
	}
	if (stat.Mode&unix.S_IFREG) != 0 && uint64(stat.Size) != numBlocks {
		err = unix.Ftruncate(fd, int64(numBlocks*BlockSize))
		if err != nil {
			return fileDisk{}, err
		}
	}
	return fileDisk{fd, numBlocks}, nil
}

// var _ Disk = FileDisk{}

func (d fileDisk) ReadTo(a uint64, buf Block) error {
	if uint64(len(buf)) != BlockSize {
		panic("buffer is not block-sized")
	}
	if a >= d.numBlocks {
		panic(fmt.Errorf("out-of-bounds read at %v", a))
	}
	_, err := unix.Pread(d.fd, buf, int64(a*BlockSize))
	if err != nil {
		panic("read failed: " + err.Error())
	}
	fmt.Printf("read: %v-%v\n", num2byte(a), buf)
	return nil
}

func (d fileDisk) Read(a uint64) (Block, error) {
	buf := make([]byte, BlockSize)
	err := d.ReadTo(a, buf)
	return buf, err
}

func (d fileDisk) Write(a uint64, v Block) error {
	if uint64(len(v)) != BlockSize {
		panic(fmt.Errorf("v is not block sized (%d bytes)", len(v)))
	}
	if a >= d.numBlocks {
		panic(fmt.Errorf("out-of-bounds write at %v", a))
	}
	_, err := unix.Pwrite(d.fd, v, int64(a*BlockSize))
	if err != nil {
		panic("write failed: " + err.Error())
	}
	fmt.Printf("write: %v-%v\n", num2byte(a), v)
	return nil
}

func (d fileDisk) Size() (uint64, error) {
	return d.numBlocks, nil
}

func (d fileDisk) Barrier() error {
	// NOTE: on macOS, this flushes to the drive but doesn't actually issue a
	// disk barrier; see https://golang.org/src/internal/poll/fd_fsync_darwin.go
	// for more details. The correct replacement is to issue a fcntl syscall with
	// cmd F_FULLFSYNC.
	err := unix.Fsync(d.fd)
	if err != nil {
		panic("file sync failed: " + err.Error())
	}
	fmt.Printf("barrier\n")
	return nil
}

func (d fileDisk) Close() error {
	err := unix.Close(d.fd)
	if err != nil {
		panic(err)
	}
	return nil
}
func (d fileDisk) WriteBatch2(startPos uint64, blocks []Block) error {
	for i, buf := range blocks {
		d.Write(startPos+uint64(i), buf)
	}
	return nil
}
func (d fileDisk) ReadBatch(startPos uint64, blockLen int) ([]Block, error) {
	var blks []Block
	for i := 0; i < blockLen; i++ {
		blk, _ := d.Read(startPos + uint64(i))
		blks = append(blks, blk)
	}
	return blks, nil
}

/////////////////////////
/////////////////////////
/////////////////////////
/////////////////////////

var _ Disk = (*memDisk)(nil)

type memDisk struct {
	l      *sync.RWMutex
	blocks [][BlockSize]byte
}

func NewMemDisk(numBlocks uint64) memDisk {
	blocks := make([][BlockSize]byte, numBlocks)
	return memDisk{l: new(sync.RWMutex), blocks: blocks}
}

func (d memDisk) ReadTo(a uint64, buf Block) error {
	d.l.RLock()
	defer d.l.RUnlock()
	if a >= uint64(len(d.blocks)) {
		panic(fmt.Errorf("out-of-bounds read at %v", a))
	}
	copy(buf, d.blocks[a][:])
	return nil
}

func (d memDisk) Read(a uint64) (Block, error) {
	buf := make(Block, BlockSize)
	d.ReadTo(a, buf)
	return buf, nil
}

func (d memDisk) Write(a uint64, v Block) error {
	if uint64(len(v)) != BlockSize {
		panic(fmt.Errorf("v is not block-sized (%d bytes)", len(v)))
	}
	d.l.Lock()
	defer d.l.Unlock()
	if a >= uint64(len(d.blocks)) {
		panic(fmt.Errorf("out-of-bounds write at %v", a))
	}
	copy(d.blocks[a][:], v)
	return nil
}

func (d memDisk) Size() (uint64, error) {
	// this never changes so we assume it's safe to run lock-free
	return uint64(len(d.blocks)), nil
}

func (d memDisk) Barrier() error { return nil }

func (d memDisk) Close() error { return nil }
func (d memDisk) WriteBatch(startPos uint64, blocks []Block) error {
	for i, buf := range blocks {
		d.Write(startPos+uint64(i), buf)
	}
	return nil
}
func (d memDisk) ReadBatch(startPos uint64, blockLen int) ([]Block, error) {
	var blks []Block
	for i := 0; i < blockLen; i++ {
		blk, _ := d.Read(startPos + uint64(i))
		blks = append(blks, blk)
	}
	return blks, nil
}
