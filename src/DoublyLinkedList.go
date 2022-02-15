package src

import (
	"encoding/binary"
	"fmt"
)

type DoublyLinkedListNode struct {
	Value    []byte
	previous *DoublyLinkedListNode
	next     *DoublyLinkedListNode
}

type DoublyLinkedList struct {
	head     *DoublyLinkedListNode
	tail     *DoublyLinkedListNode
	count    int
	maxCount int
}

func (doublyLinkedList *DoublyLinkedList) isEmpty() bool {
	return doublyLinkedList.count == 0
}

func (doublyLinkedList *DoublyLinkedList) AddFirst(value []byte) *DoublyLinkedListNode {
	newNode := DoublyLinkedListNode{
		Value:    value,
		previous: doublyLinkedList.head,
		next:     doublyLinkedList.head.next,
	}
	doublyLinkedList.head.next.previous = &newNode
	doublyLinkedList.head.next = &newNode
	doublyLinkedList.count++
	if doublyLinkedList.count > doublyLinkedList.maxCount {
		doublyLinkedList.removeLast()
	}

	return &newNode
}

func (doublyLinkedList *DoublyLinkedList) removeLast() {
	if doublyLinkedList.isEmpty() {
		return
	}

	if doublyLinkedList.count == 1 {
		doublyLinkedList.tail.previous = doublyLinkedList.head
		doublyLinkedList.head.next = doublyLinkedList.tail
	} else {
		newLast := doublyLinkedList.tail.previous.previous
		newLast.next = doublyLinkedList.tail
		doublyLinkedList.tail.previous = newLast
	}
	doublyLinkedList.count--
}

func NewDoublyLinkedList(maxcount int) DoublyLinkedList {
	head := DoublyLinkedListNode{
		Value:    nil,
		previous: nil,
		next:     nil,
	}
	tail := DoublyLinkedListNode{
		Value:    nil,
		previous: &head,
		next:     nil,
	}
	head.next = &tail

	dl := DoublyLinkedList{
		head:     &head,
		tail:     &tail,
		count:    0,
		maxCount: maxcount,
	}

	return dl

}

func (doublyLinkedList *DoublyLinkedList) swapPlaces(found *DoublyLinkedListNode) {

	found.next.previous = found.previous
	found.previous.next = found.next

	found.next = doublyLinkedList.head.next
	found.previous = doublyLinkedList.head
	doublyLinkedList.head.next.previous = found
	doublyLinkedList.head.next = found

}

func DoublyLinkedTest() {
	dl := NewDoublyLinkedList(10)

	for i := 0; i < 20; i++ {
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, uint32(i))
		dl.AddFirst(bs)
	}
	fmt.Println("done")

	test1 := dl.head.next.next.next
	dl.swapPlaces(test1)
	dl.swapPlaces(dl.head.next)
	dl.swapPlaces(dl.tail.previous)
}
