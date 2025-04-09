package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// most significant byte is stored at the lowest memory address
	// It ensures that the data is written in a consistent byte order
	enc = binary.BigEndian
)

const (
	// the number of bytes used to store the record’s length
	lenWidth = 8
)

// store of logs
type store struct {
	*os.File
	// ensure we lock the store when writing
	mu sync.Mutex
	// we store in a buffer so we're more efficient writing to our store file
	// for instance, if we have a large number of small writes, instead of constantly updating
	// the file, we can push it all together from the buffer to reduce # of system calls
	buf  *bufio.Writer
	size uint64
}

// creates a new store for the given file
func newStore(f *os.File) (*store, error) {
	// gets the current size in case we're recreating the store from an existing file, e.g. if a service had restarted
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(file.Size())
	return &store{
		File: f,
		size: size,
		// when flushing, writes to f
		buf: bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// we're writing the length of each log entry before writing the actual log data
	// We write the length of the record so that, when we read the record, we know how many bytes to read
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)
	// return bytes written
	return uint64(w), pos, nil
}

// essentially returns record stored at a given position in the file
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush writer in case we're about to try to read a record
	// that the buffer hasn't written to disk yet
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// get how many bytes we have to read to get the whole record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// get how many bytes are in the actual record
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	// reads len(p) bytes into p beginning at the off offset in the store's file
	return s.File.ReadAt(p, off)
}

// straight forward, persist any buffered data and close file ...
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
