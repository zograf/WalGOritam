package src

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"os"
)

type Node struct {
	left  *Node
	right *Node
	hash  []byte
}

func (node *Node) isLeaf() bool {
	return node.left == nil && node.right == nil
}

type MerkleTree struct {
	root *Node
}

func FormMerkle(dataArray [][]byte) MerkleTree {

	h := sha1.New()
	var hashVal []byte
	queue := Queue{
		size:  0,
		data:  make([]*Node, len(dataArray)),
		front: 0,
	}

	hashes := make([][]byte, len(dataArray))
	for i, data := range dataArray {
		h.Write(data)
		hashVal = h.Sum(nil)
		hashes[i] = hashVal

		queue.Enqueue(&Node{
			left:  nil,
			right: nil,
			hash:  hashVal,
		})
	}
	n := len(dataArray)
	var child1 *Node
	var child2 *Node

	var parentNodes []*Node
	var numOfChildren int
	var numOfParents int
	for !queue.IsEmpty() {
		// at this height we need this many parent nodes
		numOfChildren = n
		numOfParents = (numOfChildren + 1) / 2
		parentNodes = make([]*Node, numOfParents)
		// we set values for each parent
		for i := 0; i < numOfParents; i++ {
			child1 = queue.Dequeue()
			// only last element may not have it's right child and that happens when number of children is odd
			if i == numOfParents-1 && numOfChildren%2 == 1 {
				// if parent has only one child his hash is same as child's hash
				parentNodes[i] = &Node{
					left:  child1,
					right: nil,
					hash:  child1.hash,
				}
			} else {
				child2 = queue.Dequeue()
				// we combine bytes and then hash them
				h.Write(append(child1.hash, child2.hash...))
				hashVal = h.Sum(nil)
				parentNodes[i] = &Node{
					left:  child1,
					right: child2,
					hash:  hashVal,
				}
			}
			// if we enqueue root each time the loop will never end
			if queue.IsEmpty() {
				break
			}
			queue.Enqueue(parentNodes[i])

		}
	}
	return MerkleTree{root: parentNodes[0]}
}

func (merkle *MerkleTree) GetRootHash() string{
	return base64.URLEncoding.EncodeToString(merkle.root.hash)
}

func (merkle *MerkleTree) WriteMetadata(filePath string){
	file, err := os.Create(filePath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	queue := Queue{
		size:  0,
		data:  make([]*Node, 1),
		front: 0,
	}
	var currentNode *Node
	queue.Enqueue(merkle.root)
	var numOfWritten int
	for !queue.IsEmpty(){
		currentNode = queue.Dequeue()
		fmt.Println(numOfWritten)
		if err != nil {
			panic(err)
		}
		if currentNode != nil{
			if currentNode.left != nil {
				queue.Enqueue(currentNode.left)
			}
			if currentNode.right != nil {
				queue.Enqueue(currentNode.right)
			}
		}
		numOfWritten, err = file.WriteString(base64.URLEncoding.EncodeToString(currentNode.hash) + "\n")
	}
}

