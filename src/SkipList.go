package SkipList

import (
	"fmt"
	"strings"
	"time"
)

import(
	"math/rand"
)

type SkipList struct {
	maxHeight int
	height    int
	Size      int
	Head *SkipListNode
	Tail *SkipListNode
}

type SkipListNode struct {
	Key   string
	Value []byte
	next  []*SkipListNode
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
	for i:= sl.height; i >= 0; i--{
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

func MakeSkipList() SkipList{
	var lastNode SkipListNode = SkipListNode{
		next:  make([]*SkipListNode, 0),
		Value: nil,
		Key:   "inf",
	}
	rand.Seed(time.Now().Unix())
	nextNodes := make([]*SkipListNode, 0)
	nextNodes = append(nextNodes, &lastNode)
	var firstNode SkipListNode = SkipListNode{
		next:  nextNodes,
		Value: nil,
		Key:   "-inf",
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

func Greater(key1 string, key2 string) bool{
	if strings.Compare(key1, "inf") == 0{
		return true
	}else if strings.Compare(key1, "-inf") == 0{
		return false
	}else if strings.Compare(key2, "inf") == 0{
		return false
	}else if strings.Compare(key2, "-inf") == 0{
		return true
	}else{
		return key1 > key2
	}
}
func GreaterEqual(key1 string, key2 string) bool{
	if strings.Compare(key1, "inf") == 0{
		return true
	}else if strings.Compare(key1, "-inf") == 0{
		return false
	}else if strings.Compare(key2, "inf") == 0{
		return false
	}else if strings.Compare(key2, "-inf") == 0{
		return true
	}else{
		return key1 >= key2
	}
}

func (sl *SkipList) Set(key string, val []byte){

	h := sl.height
	node := sl.Head
	levelPath := make([]*SkipListNode, 0)
	for i := h; i >= 0; i--{
		for Greater(key, node.next[i].Key) {
			node = node.next[i]
		}
		// cuva se putanja kojom se islo, odnosno poslednji(skroz desno) cvor kojim smo se kretali kroz listu
		//na svakom nivou, bice korisno kasnije
		levelPath = append(levelPath, node)
	}

	//okrene se redosled putanje posto Go nema prepend i krecemo od visine 0
	levelPath = reverseSlice(levelPath)

	//ukoliko taj kljuc vec postoji samo se menja vrednost
	if levelPath[0].next[0].Key == key{
		levelPath[0].next[0].Value = val
		return
	}

	//uzima se nasumicna vrednost za nivo propagacije
	lvl := sl.roll()
	//fmt.Println(lvl)


	// dodaju se potrebne liste koje povezuju head i Tail ukoliko je visina veca od predjasnje
	if lvl > sl.height{
		for i := sl.height + 1; i <= lvl; i++{
			sl.Head.next = append(sl.Head.next, sl.Tail)
			sl.height ++
			levelPath = append(levelPath, sl.Head)
		}
	}

	// pravi se novi cvor(lvl + 1 jer lvl moze biti 0)
	newNode := SkipListNode{
		Key:   key,
		Value: val,
		next:  make([]*SkipListNode, lvl + 1),
	}

	//prevezivanje sledecih cvorova i novog cvora
	for i := 0; i <= lvl; i++{
		newNode.next[i] = levelPath[i].next[i]
		levelPath[i].next[i] = &newNode
	}
	sl.Size += 64 + len(val)
}

//okrece elemente u slice-u u obrnutom redosledu
func reverseSlice(s []*SkipListNode) []*SkipListNode {
	for i, j := 0, len(s) - 1; i < j; i, j = i + 1, j - 1{
		s[i], s[j] = s[j], s[i]
	}
	return s
}

//nalazi vrednost cvora sa vrednoscu Key
func(sl *SkipList) GetVal(key string) []byte{
	h := sl.height
	node := sl.Head
	for i := h; i >= 0; i-- {
		for GreaterEqual(key, node.next[i].Key) {
			node = node.next[i]
		}
	}
	return node.Value
}

//nalazi cvor sa vrednoscu Key
func(sl *SkipList) SearchNode(key string) *SkipListNode{
	h := sl.height
	node := sl.Head
	for i := h; i >= 0; i-- {
		for GreaterEqual(key, node.next[i].Key) {
			node = node.next[i]
		}
	}

	if node.Key == key{
		return node
	}else{
		return nil
	}
}

func(sl *SkipList) Delete(key string){
	h := sl.height
	node := sl.Head
	levelPath := make([]*SkipListNode, 0)
	for i := h; i >= 0; i--{
		for GreaterEqual(key, node.next[i].Key) {
			node = node.next[i]
		}
		// cuva se putanja kojom se islo, odnosno poslednji(skroz desno) cvor kojim smo se kretali kroz listu
		//na svakom nivou, bice korisno kasnije
		levelPath = append(levelPath, node)
	}
	reverseSlice(levelPath)
	// svi u level path su bili manji, proveravamo  da li je sledeci cvor trazeni cvor
	node = levelPath[0].next[0]
	if node.Key != key{
		panic("No such Key in skip list")
	}
	//trebalo bi len(node.next) - 1
	for i := len(node.next) - 2; i >= 0; i-- {
		levelPath[i].next[i] = node.next[i]
	}
}
func (sl *SkipList) CreateIterator() SkipListIterator{
	return SkipListIterator{
		start: sl.Head,
		end:   sl.Tail,
		curr:  sl.Head,
	}
}


type SkipListIterator struct {
	start *SkipListNode
	end *SkipListNode
	curr *SkipListNode
}

func(sli *SkipListIterator) HasNext() bool{
	return sli.curr.next[0] != sli.end
}

func(sli *SkipListIterator) GetNext() *SkipListNode{
	if sli.HasNext(){
		sli.curr = sli.curr.next[0]
		return sli.curr
	}
	return nil
}
