package server

import (
	"fmt"
	"sync"
)

type Log struct {
	// have a mutex to ensure proper ordering of our records
	mu sync.Mutex
	// sequential list of our logs
	records []Record
}

func NewLog() *Log {
	return &Log{}
}
func (c *Log) Append(record Record) (uint64, error) {
	// lock our resource
	c.mu.Lock()
	// defers unlocking of mutex until the function returns
	defer c.mu.Unlock()
	// set the record's offset in accordance to our stored logs
	record.Offset = uint64(len(c.records))
	// add the record to our stored logs
	c.records = append(c.records, record)
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	// lock our resource
	c.mu.Lock()
	// defers unlocking of mutex until the function returns
	defer c.mu.Unlock()
	// if the log we're looking for is outside of our true range, err out
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	//return record at given offset
	return c.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
