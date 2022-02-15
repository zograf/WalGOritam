package src

import "errors"

// Queue - circular queue implementation
type Queue struct {
	size  int
	data  []*Node
	front int
}

// returns size of queue
func (aq *Queue) len() int {
	return aq.size
}

// IsEmpty - returns  whether queue is empty
func (aq *Queue) IsEmpty() bool {
	return aq.size == 0
}

// First - returns first element in queue without its removal
func (aq *Queue) First() *Node {
	if aq.IsEmpty() {
		panic(errors.New("queue is empty"))
	}
	return aq.data[aq.front]
}

// Dequeue - returns first element with its removal
func (aq *Queue) Dequeue() *Node {
	if aq.IsEmpty() {
		panic(errors.New("queue is empty"))
	}
	result := aq.data[aq.front]
	aq.data[aq.front] = nil
	aq.front = (aq.front + 1) % len(aq.data)
	aq.size--
	return result
}

// resizes data array for given size
func (aq *Queue) resize(newSize int) {
	old := aq.data
	aq.data = make([]*Node, newSize)
	oldFirst := aq.front
	// we copy old array in new one
	for i := 0; i < aq.size; i++ {
		aq.data[i] = old[oldFirst]
		oldFirst = (oldFirst + 1) % len(old)
	}
	aq.front = 0
}

// Enqueue - adds new element at the end of queue data array
func (aq *Queue) Enqueue(val *Node) {
	// if array size is smaller than we can comprehend we double the size
	if aq.size == len(aq.data) {
		aq.resize(2 * len(aq.data))
	}
	// we find position in circular array
	newElemPosition := (aq.front + aq.size) % len(aq.data)
	aq.data[newElemPosition] = val
	aq.size++
}
