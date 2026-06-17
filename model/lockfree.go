package model

import (
	"sync/atomic"
)

type LockFreeQueue struct {
	head atomic.Pointer[lfqNode]
	tail atomic.Pointer[lfqNode]
	len  int64
}

type lfqNode struct {
	value interface{}
	next  atomic.Pointer[lfqNode]
}

func NewLockFreeQueue() *LockFreeQueue {
	stub := &lfqNode{}
	q := &LockFreeQueue{}
	q.head.Store(stub)
	q.tail.Store(stub)
	return q
}

func (q *LockFreeQueue) Enqueue(value interface{}) {
	node := &lfqNode{value: value}
	for {
		tail := q.tail.Load()
		next := tail.next.Load()
		if next == nil {
			if tail.next.CompareAndSwap(nil, node) {
				q.tail.CompareAndSwap(tail, node)
				atomic.AddInt64(&q.len, 1)
				return
			}
		} else {
			q.tail.CompareAndSwap(tail, next)
		}
	}
}

func (q *LockFreeQueue) Dequeue() (interface{}, bool) {
	for {
		head := q.head.Load()
		next := head.next.Load()
		if next == nil {
			return nil, false
		}
		if q.head.CompareAndSwap(head, next) {
			val := next.value
			next.value = nil
			atomic.AddInt64(&q.len, -1)
			return val, true
		}
	}
}

func (q *LockFreeQueue) Len() int64 {
	return atomic.LoadInt64(&q.len)
}
