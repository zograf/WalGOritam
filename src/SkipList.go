package src

import (
	"fmt"
	"math"
	"time"
)

import(
	"math/rand"
)

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
	tail      *SkipListNode
}

type SkipListNode struct {
	key       int64
	value     []byte
	next      []*SkipListNode
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

func (sl *SkipList) print() {
	for i:= sl.height; i >= 0; i--{
		node := sl.head.next[i]
		for node != sl.tail{
			fmt.Print(node.key)
			fmt.Print("----->")

			node = node.next[i]
		}
		fmt.Println()
	}
}

func MakeSkipList() SkipList{
	var lastNode SkipListNode = SkipListNode{
		next:  make([]*SkipListNode, 0),
		value: nil,
		key:   math.MaxInt64,
	}
	rand.Seed(time.Now().Unix())
	nextNodes := make([]*SkipListNode, 0)
	nextNodes = append(nextNodes, &lastNode)
	var firstNode SkipListNode = SkipListNode{
		next:  nextNodes,
		value: nil,
		key:   math.MinInt64,
	}
	
	sl := SkipList{
		maxHeight: 100,
		height:    0,
		size:      0,
		head:      &firstNode,
		tail: &lastNode,
	}

	return sl
}

func (sl *SkipList) Add(key int64, val []byte){
	h := sl.height
	node := sl.head
	levelPath := make([]*SkipListNode, 0)
	for i := h; i >= 0; i--{
		for key > node.next[i].key {
			node = node.next[i]
		}
		// cuva se putanja kojom se islo, odnosno poslednji(skroz desno) cvor kojim smo se kretali kroz listu
		//na svakom nivou, bice korisno kasnije
		levelPath = append(levelPath, node)
	}

	//okrene se redosled putanje posto Go nema prepend i krecemo od visine 0
	levelPath = reverseSlice(levelPath)

	//ukoliko taj kljuc vec postoji samo se menja vrednost
	if levelPath[0].next[0].key == key{
		levelPath[0].next[0].value = val
		return
	}

	//uzima se nasumicna vrednost za nivo propagacije
	lvl := sl.roll()
	fmt.Println(lvl)


	// dodaju se potrebne liste koje povezuju head i tail ukoliko je visina veca od predjasnje
	if lvl > sl.height{
		for i := sl.height + 1; i <= lvl; i++{
			sl.head.next = append(sl.head.next, sl.tail)
			sl.height ++
			levelPath = append(levelPath, sl.head)
		}
	}

	// pravi se novi cvor(lvl + 1 jer lvl moze biti 0)
	newNode := SkipListNode{
		key:   key,
		value: val,
		next:  make([]*SkipListNode, lvl + 1),
	}

	//prevezivanje sledecih cvorova i novog cvora
	for i := 0; i <= lvl; i++{
		newNode.next[i] = levelPath[i].next[i]
		levelPath[i].next[i] = &newNode
	}

}

//okrece elemente u slice-u u obrnutom redosledu
func reverseSlice(s []*SkipListNode) []*SkipListNode {
	for i, j := 0, len(s) - 1; i < j; i, j = i + 1, j - 1{
		s[i], s[j] = s[j], s[i]
	}
	return s
}

//nalazi vrednost cvora sa vrednoscu key
func(sl *SkipList) SearchVal(key int64) []byte{
	h := sl.height
	node := sl.head
	for i := h; i >= 0; i-- {
		for key >= node.next[i].key {
			node = node.next[i]
		}
	}
	return node.value
}

//nalazi cvor sa vrednoscu key
func(sl *SkipList) SearchNode(key int64) *SkipListNode{
	h := sl.height
	node := sl.head
	for i := h; i >= 0; i-- {
		for key >= node.next[i].key {
			node = node.next[i]
		}
	}

	if node.key == key{
		return node
	}else{
		return nil
	}
}

func(sl *SkipList) Delete(key int64){
	h := sl.height
	node := sl.head
	levelPath := make([]*SkipListNode, 0)
	for i := h; i >= 0; i--{
		for key > node.next[i].key {
			node = node.next[i]
		}
		// cuva se putanja kojom se islo, odnosno poslednji(skroz desno) cvor kojim smo se kretali kroz listu
		//na svakom nivou, bice korisno kasnije
		levelPath = append(levelPath, node)
	}
	reverseSlice(levelPath)
	// svi u level path su bili manji, proveravamo  da li je sledeci cvor trazeni cvor
	node = levelPath[0].next[0]
	if node.key != key{
		panic("No such key in skip list")
	}
	//trebalo bi len(node.next) - 1
	for i := len(node.next) - 2; i >= 0; i-- {
		levelPath[i].next[i] = node.next[i]
	}
}

