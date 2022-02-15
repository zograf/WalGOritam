package src

import (
	"encoding/gob"
	"hash"
	"math"
	"os"
	"time"

	"github.com/spaolacci/murmur3"
)

// Helper functions
func (bf *BloomFilter) calculateM(expectedElements int, falsePositiveRate float64) uint32 {
	return uint32(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func (bf *BloomFilter) calculateK(expectedElements int, m uint32) uint32 {
	return uint32(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func (bf *BloomFilter) createHashFunctions(k, seed uint32) []hash.Hash32 {
	h := []hash.Hash32{}
	for i := uint32(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(seed+i)))
	}
	return h
}

type BloomFilter struct {
	K         uint32
	M         uint32
	Seed      uint32
	hashArray []hash.Hash32
	Memory    []byte
}

func newBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	bfInstance := BloomFilter{}
	bfInstance.Seed = uint32(time.Now().Unix())
	bfInstance.M = bfInstance.calculateM(expectedElements, falsePositiveRate)
	bfInstance.K = bfInstance.calculateK(expectedElements, bfInstance.M)
	bfInstance.hashArray = bfInstance.createHashFunctions(bfInstance.K, bfInstance.Seed)
	bfInstance.Memory = make([]byte, bfInstance.M)
	return &bfInstance
}

// Hashes the element with every hashFunction and returns an array
//of indexes as an indicator of where to write 1 instead of 0
func (bf *BloomFilter) hash(element string) []uint32 {
	hashArray := make([]uint32, 0)
	for _, hashFunction := range bf.hashArray {
		_, err := hashFunction.Write([]byte(element))
		check(err)
		index := hashFunction.Sum32() % bf.M
		hashArray = append(hashArray, index)
		hashFunction.Reset()
	}
	return hashArray
}

func (bf *BloomFilter) add(element string) {
	for _, index := range bf.hash(element) {
		bf.Memory[index] = 1
	}
}

func (bf *BloomFilter) isInBloomFilter(element string) bool {
	for _, index := range bf.hash(element) {
		if bf.Memory[index] != 1 {
			return false
		}
	}
	return true
}

// Serializing
func encodeBloomFilter(bf *BloomFilter, path string) {
	file, err := os.Create(path)
	check(err)
	encoder := gob.NewEncoder(file)
	encoder.Encode(bf)
	file.Close()
}

// Deserializing
func decodeBloomFilter(path string) *BloomFilter {
	file, err := os.Open(path)
	check(err)
	decoder := gob.NewDecoder(file)
	var bf BloomFilter
	err = decoder.Decode(&bf)
	bf.hashArray = bf.createHashFunctions(bf.K, bf.Seed)
	file.Close()
	return &bf
}

/*func main() {
	Testing
	gob.Register(BloomFilter{})
	bf := newBloomFilter(10, 0.1)
	bf.add("uros")
	bf.add("nemanja")
	fmt.Println(bf)
	path := "../res/bloomfilterexample.gob"
	encodeBloomFilter(bf, path)
	bfDecoded := decodeBloomFilter(path)
	fmt.Println(bfDecoded)
	fmt.Println(bfDecoded.isInBloomFilter("uros"))
	fmt.Println(bfDecoded.isInBloomFilter("lazar"))
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
*/
