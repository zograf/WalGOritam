package src

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type SkipList struct {
	maxHeight int
	height    int
	Size      int
	Head      *SkipListNode
	Tail      *SkipListNode
}

type SkipListNode struct {
	Key       string
	Value     []byte
	next      []*SkipListNode
	Tombstone byte
}

func (s *SkipList) roll() int {
	var level int

	for level = 0; rand.Intn(2) == 1; level++ {
		if level >= s.maxHeight {
			break
		}
	}
	return level
}

func (sl *SkipList) Print() {
	for i := sl.height; i >= 0; i-- {
		node := sl.Head.next[i]
		for node != sl.Tail {
			fmt.Print(node.Key)
			fmt.Print("----->")

			node = node.next[i]
		}
		fmt.Println()
	}
	fmt.Println()
	fmt.Println()
}

func MakeSkipList() SkipList {
	var lastNode SkipListNode = SkipListNode{
		next:      make([]*SkipListNode, 0),
		Value:     nil,
		Key:       "inf",
		Tombstone: 0,
	}
	rand.Seed(time.Now().Unix())
	nextNodes := make([]*SkipListNode, 0)
	nextNodes = append(nextNodes, &lastNode)
	var firstNode SkipListNode = SkipListNode{
		next:      nextNodes,
		Value:     nil,
		Key:       "-inf",
		Tombstone: 0,
	}

	sl := SkipList{
		maxHeight: 100,
		height:    0,
		Size:      0,
		Head:      &firstNode,
		Tail:      &lastNode,
	}

	return sl
}

func Greater(key1 string, key2 string) bool {
	if strings.Compare(key1, "inf") == 0 {
		return true
	} else if strings.Compare(key1, "-inf") == 0 {
		return false
	} else if strings.Compare(key2, "inf") == 0 {
		return false
	} else if strings.Compare(key2, "-inf") == 0 {
		return true
	} else {
		return key1 > key2
	}
}
func GreaterEqual(key1 string, key2 string) bool {
	if strings.Compare(key1, "inf") == 0 {
		return true
	} else if strings.Compare(key1, "-inf") == 0 {
		return false
	} else if strings.Compare(key2, "inf") == 0 {
		return false
	} else if strings.Compare(key2, "-inf") == 0 {
		return true
	} else {
		return key1 >= key2
	}
}

func (sl *SkipList) Set(key string, val []byte, tombstone byte) error {
	if strings.Compare(key, "inf") == 0 || strings.Compare(key, "-inf") == 0 {
		return errors.New("invalid key input")
	}

	h := sl.height
	node := sl.Head
	levelPath := make([]*SkipListNode, 0)
	for i := h; i >= 0; i-- {
		for Greater(key, node.next[i].Key) {
			node = node.next[i]
		}
		// we save path where we went, that is the last or the most right node on each level, will be useful in future
		levelPath = append(levelPath, node)
	}

	// we change the order of path since Go doesn't have prepend and we start from height zero
	levelPath = reverseSlice(levelPath)

	// if key exists we just change the value
	if levelPath[0].next[0].Key == key {
		levelPath[0].next[0].Value = val
		return nil
	}

	// we take random number for propagation level
	lvl := sl.roll()

	// we add required empty lists that didn't exist, that connect Head and Tail if height is bigger than previous
	if lvl > sl.height {
		for i := sl.height + 1; i <= lvl; i++ {
			sl.Head.next = append(sl.Head.next, sl.Tail)
			sl.height++
			levelPath = append(levelPath, sl.Head)
		}
	}

	// we make new node(lvl + 1 because lvl can be 0)
	newNode := SkipListNode{
		Key:       key,
		Value:     val,
		next:      make([]*SkipListNode, lvl+1),
		Tombstone: tombstone,
	}

	//inserting between previous and next nodes
	for i := 0; i <= lvl; i++ {
		newNode.next[i] = levelPath[i].next[i]
		levelPath[i].next[i] = &newNode
	}
	sl.Size++
	return nil
}

//reversing the order of slice
func reverseSlice(s []*SkipListNode) []*SkipListNode {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

// we find value of node with specified key
func (sl *SkipList) GetVal(key string) ([]byte, bool) {
	h := sl.height
	node := sl.Head
	for i := h; i >= 0; i-- {
		for GreaterEqual(key, node.next[i].Key) {
			node = node.next[i]
		}
	}
	if node.Tombstone == 0 {
		if node.Key == key {
			return node.Value, true
		} else {
			return nil, false
		}
	} else {
		return nil, false
	}
}

//we find node with specified key
func (sl *SkipList) SearchNode(key string) *SkipListNode {
	h := sl.height
	node := sl.Head
	for i := h; i >= 0; i-- {
		for GreaterEqual(key, node.next[i].Key) {
			node = node.next[i]
		}
	}

	if node.Key == key && node.Tombstone == 0 {
		return node
	} else {
		return nil
	}
}

func (sl *SkipList) Delete(key string) bool {
	node := sl.SearchNode(key)
	if node == nil {
		return false
	}
	node.Tombstone = 1
	return true
}
func (sl *SkipList) CreateIterator() SkipListIterator {
	return SkipListIterator{
		start: sl.Head,
		end:   sl.Tail,
		curr:  sl.Head,
	}
}

type SkipListIterator struct {
	start *SkipListNode
	end   *SkipListNode
	curr  *SkipListNode
}

func (sli *SkipListIterator) HasNext() bool {
	return sli.curr.next[0] != sli.end
}

func (sli *SkipListIterator) GetNext() *SkipListNode {
	if sli.HasNext() {
		sli.curr = sli.curr.next[0]
		return sli.curr
	}
	return nil
}
