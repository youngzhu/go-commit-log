package log

import (
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	//err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	//if err != nil {
	//	return nil, err
	//}
	if err = os.Truncate(
		f.Name(),
		int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

// Read takes in an offset and returns the associated record's
// position in the store
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

// Name return the index's file path
func (i *index) Name() string {
	return i.file.Name()
}

// Close makes sure the memory-mapped file has synced its data
// to the persisted file and that the persisted file has flushed
// its contents to stable storage. Then it truncates the persisted
// file to the amount of data that's actually in it and closes the file
func (i *index) Close() (err error) {
	defer func() {
    if cerr := i.file.Close(); cerr != nil {
        err = cerr // 如果出现错误，则将其赋值给函数返回值
    }
}()
	
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	// 无法执行，报错：
	// The requested operation cannot be performed on a file with a user-mapped section open.
	//if err := i.file.Truncate(int64(i.size)); err != nil {
	//	return err
	//}
	if err := os.Truncate(i.Name(), int64(i.size)); err != nil {
		return err
	}

	return 
}
