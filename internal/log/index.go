package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// same as before, we will keep constants at the top
	offWidth uint64 = 4 // the records offset
	posWidth uint64 = 8 // the records position in store file
	totWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap // memory-mapped file
	size uint64
}

// creates an index for a given file; create the index and save the current size of the file
// so we can track the amount of data in the index file as we add more indices.
// we grow the file to the max index size before memory-mapping the file and return the created
// index to the caller
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// gets the current size in case we're recreating the store from an existing file, e.g. if a service had restarted
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(file.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// memory-maps the file and returns the index based on the above file
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// ensure our mmap file synced its data to our persisted index file
// and the persisted file flushed its content to stable storage
// when we memory map, we must set the size initially which makes us unable
// to get our last index due to the empty space between that index and the rest of the file size
// so we truncate the file based on the true index size
func (i *index) Close() error {
	// equivalent of flushing a buffer but for our memory map ...
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// flushes the in-memory copy of the file
	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return nil
	}

	return i.file.Close()
}

// we take in an offset and returns the record's position in our store file
// the offset is relative to the segment's base offset. The indices are 0-indexed
// we use uint32 for our offsets instead of uint64 as 4 bytes can go a long way
// when we have trillions of records ...
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / totWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * totWidth
	if i.size < pos+totWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+totWidth])
	return out, pos, nil
}

// pretty straight forward, given the offset and position,
// write into our mmap the corresponding values
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+totWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+totWidth], pos)
	i.size += uint64(totWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
