package src

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

type Cache struct {
	lruList DoublyLinkedList
	lruMap  map[string]*DoublyLinkedListNode
	size    int
}

func NewCache(size int) *Cache {
	c := Cache{
		lruList: NewDoublyLinkedList(size),
		lruMap:  make(map[string]*DoublyLinkedListNode),
		size:    size,
	}
	c.Put("", nil)
	return &c
}

func (cache *Cache) Search(key string) []byte {
	node, found := cache.lruMap[key]
	if found {
		cache.lruList.swapPlaces(node)
		return node.Value
	} else {
		return nil
	}
}

func (cache *Cache) Put(key string, value []byte) {
	newNode, deletedKey := cache.lruList.AddFirst(key, value)
	if deletedKey != "" {
		delete(cache.lruMap, deletedKey)
	}
	cache.lruMap[key] = newNode
}
func (cache *Cache) DeleteElement(key string) {
	node, found := cache.lruMap[key]
	if found {
		node.previous.next = node.next
		node.next.previous = node.previous
	}

}

func TestCache() {
	cache := NewCache(10)

	for i := 0; i < 10; i++ {
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, uint32(i))
		cache.Put(strconv.Itoa(i), bs)
	}
	cache.Search("2")
	cache.Search("5")
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(25))
	cache.Put("25", bs)
	fmt.Println("Done")

}
