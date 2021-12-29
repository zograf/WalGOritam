package main

import (
	"encoding/gob"
	"hash"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/spaolacci/murmur3"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	M            uint64
	P            uint8
	Registers    []uint8
	hashFunction hash.Hash32
}

func newHLL(p uint8) *HLL {
	hll := HLL{}
	hll.P = p
	hll.M = uint64(math.Pow(2, float64(p)))
	hll.hashFunction = hllCreateHashFunction()
	hll.Registers = make([]uint8, hll.M)
	return &hll
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Registers {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func hllCreateHashFunction() hash.Hash32 {
	seed := time.Now().Unix()
	hashFunction := murmur3.New32WithSeed(uint32(seed))
	return hashFunction
}

func (hll *HLL) hash(element string) uint32 {
	hll.hashFunction.Write([]byte(element))
	value := hll.hashFunction.Sum32()
	hll.hashFunction.Reset()
	return value
}

func (hll *HLL) add(element string) {
	binaryRepresentation := strconv.FormatUint(uint64(hll.hash(element)), 2)
	leading := binaryRepresentation[:hll.P]
	bucketIndex, err := strconv.ParseUint(leading, 2, 32)
	check(err)
	bucketValue := uint8(0)
	for i := len(binaryRepresentation) - 1; i > 0; i-- {
		if binaryRepresentation[i] != '0' {
			break
		}
		bucketValue++
	}
	hll.Registers[bucketIndex] = bucketValue
}

// Serializing
func encodeHLL(hll *HLL, path string) {
	file, err := os.Create(path)
	check(err)
	encoder := gob.NewEncoder(file)
	encoder.Encode(hll)
	file.Close()
}

// Deserializing
func decodeHLL(path string) *HLL {
	file, err := os.Open(path)
	check(err)
	decoder := gob.NewDecoder(file)
	var hll HLL
	err = decoder.Decode(&hll)
	check(err)
	hll.hashFunction = hllCreateHashFunction()
	file.Close()
	return &hll
}

/*
func main() {
	hllInstance := newHLL(4)
	hllInstance.add("lazo")
	hllInstance.add("neco")
	hllInstance.add("uky")
	fmt.Println(hllInstance.M)
	fmt.Println(hllInstance.P)
	fmt.Println(hllInstance.Registers)
	path := "../res/hyperloglog.gob"
	encodeHLL(hllInstance, path)
	newHLL := decodeHLL(path)
	fmt.Println(newHLL.M)
	fmt.Println(newHLL.P)
	fmt.Println(newHLL.Registers)
}
func check(e error) {
	if e != nil {
		panic(e)
	}
}
*/

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Registers {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}
