package disk

// Block is a 4096-byte buffer
type Block = []byte

const BlockSize uint64 = 4096 // 128 //128 // 32 // 4096

// Disk provides access to a logical block-based disk
type Disk interface {
	// Read reads a disk block by address
	//
	// Expects a < Size().
	Read(a uint64) (Block, error)

	// ReadTo reads the disk block at a and stores the result in b
	//
	// Expects a < Size().
	ReadTo(a uint64, b Block) error

	// Write updates a disk block by address
	//
	// Expects a < Size().
	Write(a uint64, v Block) error

	// Size reports how big the disk is, in blocks
	Size() (uint64, error)

	// Barrier ensures data is persisted.
	//
	// When it returns, all outstanding writes are guaranteed to be durably on
	// disk
	Barrier() error

	// Close releases any resources used by the disk and makes it unusable.
	Close() error
}

type DiskWriteBatch interface {
	WriteBatch(startPos uint64, blocks []Block) error
}

// type DiskReadBatch interface {
// 	ReadBatch(startPos uint64, blockLen int) ([]Block, error)
// }
