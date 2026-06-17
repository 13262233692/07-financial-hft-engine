package model

import (
	"sync/atomic"
)

type RingBuffer struct {
	buf   []interface{}
	size  uint64
	mask  uint64
	head  uint64
	tail  uint64
	count uint64
}

func NewRingBuffer(size uint64) *RingBuffer {
	realSize := uint64(1)
	for realSize < size {
		realSize <<= 1
	}
	return &RingBuffer{
		buf:  make([]interface{}, realSize),
		size: realSize,
		mask: realSize - 1,
	}
}

func (rb *RingBuffer) Push(item interface{}) bool {
	if atomic.LoadUint64(&rb.count) >= rb.size {
		return false
	}
	tail := atomic.LoadUint64(&rb.tail)
	rb.buf[tail&rb.mask] = item
	atomic.AddUint64(&rb.tail, 1)
	atomic.AddUint64(&rb.count, 1)
	return true
}

func (rb *RingBuffer) Pop() (interface{}, bool) {
	if atomic.LoadUint64(&rb.count) == 0 {
		return nil, false
	}
	head := atomic.LoadUint64(&rb.head)
	item := rb.buf[head&rb.mask]
	rb.buf[head&rb.mask] = nil
	atomic.AddUint64(&rb.head, 1)
	atomic.AddUint64(&rb.count, ^uint64(0))
	return item, true
}

func (rb *RingBuffer) Len() uint64 {
	return atomic.LoadUint64(&rb.count)
}
