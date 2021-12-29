package main

import (
	"encoding/gob"
	"hash"
	"math"
	"os"
	"time"

	"github.com/spaolacci/murmur3"
)

type CountMinSketch struct {
	M         uint32
	K         uint32
	Seed      uint32
	Memory    [][]uint32
	hashArray []hash.Hash32
}

func newCountMinSketch(epsilon, delta float64) *CountMinSketch {
	cmsInstance := CountMinSketch{}
	cmsInstance.M = countMinSketchCalculateM(epsilon)
	cmsInstance.K = countMinSketchCalculateK(delta)
	cmsInstance.Seed = uint32(time.Now().Unix())
	cmsInstance.hashArray = countMinSketchCreateHashFunctions(cmsInstance.K, cmsInstance.Seed)
	cmsInstance.Memory = make([][]uint32, cmsInstance.K)
	for i := range cmsInstance.Memory {
		cmsInstance.Memory[i] = make([]uint32, cmsInstance.M)
	}
	return &cmsInstance
}

func (cms *CountMinSketch) hash(element string) []uint32 {
	hashed := make([]uint32, 0)
	for _, hash := range cms.hashArray {
		hash.Write([]byte(element))
		column := hash.Sum32() % cms.M
		hash.Reset()
		hashed = append(hashed, column)
	}
	return hashed
}

func (cms *CountMinSketch) add(element string) {
	hashed := cms.hash(element)
	for row, column := range hashed {
		cms.Memory[row][column] += 1
	}
}

func (cms *CountMinSketch) find(element string) uint32 {
	hashed := cms.hash(element)
	min := uint32(math.MaxUint32)
	for row, column := range hashed {
		if cms.Memory[row][column] < min {
			min = cms.Memory[row][column]
		}
	}
	return min

}

func countMinSketchCalculateM(epsilon float64) uint32 {
	return uint32(math.Ceil(math.E / epsilon))
}

func countMinSketchCalculateK(delta float64) uint32 {
	return uint32(math.Ceil(math.Log(math.E / delta)))
}

func countMinSketchCreateHashFunctions(k, seed uint32) []hash.Hash32 {
	h := []hash.Hash32{}
	for i := uint32(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(seed+i)))
	}
	return h
}

func encodeCountMinSketch(cms *CountMinSketch, path string) {
	file, err := os.Create(path)
	check(err)
	encoder := gob.NewEncoder(file)
	encoder.Encode(cms)
	file.Close()
}

// Deserializing
func decodeCountMinSketch(path string) *CountMinSketch {
	file, err := os.Open(path)
	check(err)
	decoder := gob.NewDecoder(file)
	var cms CountMinSketch
	err = decoder.Decode(&cms)
	cms.hashArray = countMinSketchCreateHashFunctions(cms.K, cms.Seed)
	file.Close()
	return &cms
}

/*
func main() {
	cms := newCountMinSketch(0.01, 0.01)
	cms.add("lazar")
	cms.add("uros")
	cms.add("lazar")
	cms.add("nemanja")
	cms.add("nemanja")
	cms.add("lazar")
	cms.add("uros")
	cms.add("lazar")
	cms.add("nemanja")
	cms.add("uros")
	path := "../res/countminsketch.gob"
	encodeCountMinSketch(cms, path)
	cmsDecoded := decodeCountMinSketch(path)
	fmt.Println(cmsDecoded.find("nemanja"))
	fmt.Println(cmsDecoded.find("uros"))
	fmt.Println(cmsDecoded.find("lazar"))
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
*/
