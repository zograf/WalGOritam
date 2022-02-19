package src

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Engine struct {
	tokenBucket *TokenBucket
	wal         *Wal
	memTable    *Memtable
	lsm         *LSM
	cache       *Cache
	hll         *HLL
	cms         *CountMinSketch
}

func (engine *Engine) EnginePut(key, value string) {
	if strings.Compare(key, "inf") == 0 || strings.Compare(key, "-inf") == 0 {
		fmt.Println("Invalid key")
		return
	}
	err := engine.tokenBucket.CheckBucket()
	if err != nil {
		fmt.Println("You have no tokens left!")
		return
	}
	byteValue := []byte(value)
	engine.wal.put(key, byteValue)
	engine.wal.deleteSegments()
	flag := engine.memTable.Set(key, byteValue, 0)
	if flag {
		engine.lsm.Run()
	}
	fmt.Println("SUCCESS! Key-Value pair { " + key + " : " + value + " }")
}

func (engine *Engine) EngineGet(key string) ([]byte, bool) {
	fmt.Println("GET")
	val := engine.memTable.Get(key)
	if val != nil {
		return val, true
	}
	val = engine.cache.Search(key)
	if val != nil {
		return val, true
	}
	var currentData []string
	var currentIndex []string
	var currentSummary []string
	var currentFilter []string
	var summaryFile *os.File
	var err error
	var indexOffset uint32
	var found bool
	var indexEntry *IndexEntry
	var dataEntry Entry
	var filter *BloomFilter
	var isBetween bool
	var indexFile *os.File
	biggestTimestampEntry := Entry{
		CRC:       0,
		Timestamp: 0,
		Tombstone: 0,
		ValueSize: 0,
		key:       "",
		KeySize:   0,
		value:     nil,
	}
	var indexIt IndexIterator
	found = false

	for level := 1; level < Config.LsmMaxLevels; level++ {
		currentData, currentIndex, currentSummary, currentFilter, _, _ = engine.lsm.GetDataIndexSummary(level)
		for j := 0; j < len(currentData); j++ {
			filter = DecodeBloomFilter("res" + string(os.PathSeparator) + currentFilter[j])
			if !filter.IsInBloomFilter(key) {
				continue
			}
			summaryFile, err = os.OpenFile("res"+string(os.PathSeparator)+currentSummary[j], os.O_RDONLY, 0777)
			if err != nil {
				panic(err)
			}
			indexOffset, isBetween = Search(key, summaryFile)
			summaryFile.Close()
			if !isBetween {
				continue
			}
			indexFile, err = os.OpenFile("res"+string(filepath.Separator)+currentIndex[j], os.O_RDWR, 0777)
			if err != nil {
				panic(err)
			}
			indexIt = IndexIterator{file: indexFile}
			indexIt.PositionIterator(indexOffset)
			for i := 0; i < Config.BlockSize; i++ {
				if !indexIt.HasNext() {
					break
				}
				indexEntry = indexIt.GetNext()
				if indexEntry.Key > key {
					break
				}
				dataEntry = ReadDataRow(string(os.PathSeparator)+currentData[j], indexEntry.Offset)
				if dataEntry.Tombstone == 1 {
					return nil, false
				}
				if dataEntry.key == key && dataEntry.Timestamp > biggestTimestampEntry.Timestamp {
					biggestTimestampEntry = dataEntry
					found = true
				}
			}
			indexFile.Close()
		}
	}
	if found {
		if biggestTimestampEntry.Tombstone == 1 {
			return nil, false
		} else {
			engine.cache.Put(biggestTimestampEntry.key, biggestTimestampEntry.value)
			return biggestTimestampEntry.value, true
		}
	}
	return nil, found

}

func (engine *Engine) EngineDelete(key string) bool {
	fmt.Println("DEL")
	val := engine.memTable.Get(key)
	if val != nil {
		engine.wal.delete(key)
		return true
	}
	engine.cache.DeleteElement(key)

	var currentData []string
	var currentIndex []string
	var currentSummary []string
	var currentFilter []string
	var file *os.File
	var err error
	var indexOffset uint32
	var found bool
	var indexEntry *IndexEntry
	var dataEntry Entry
	var filter *BloomFilter
	for level := 1; level < Config.LsmMaxLevels; level++ {
		currentData, currentIndex, currentSummary, currentFilter, _, _ = engine.lsm.GetDataIndexSummary(level)
		for j := 0; j < len(currentData); j++ {
			filter = DecodeBloomFilter("res" + string(os.PathSeparator) + currentFilter[j])
			if !filter.IsInBloomFilter(key) {
				continue
			}
			file, err = os.OpenFile("res"+string(os.PathSeparator)+currentSummary[j], os.O_RDONLY, 0777)
			if err != nil {
				panic(err)
			}
			indexOffset, found = Search(key, file)
			if !found {
				continue
			}
			indexEntry = ReadIndexRow(string(os.PathSeparator)+currentIndex[j], indexOffset)
			dataEntry = ReadDataRow(string(os.PathSeparator)+currentData[j], indexEntry.Offset)
			file, _ := os.OpenFile(string(os.PathSeparator)+currentData[j], os.O_RDWR, 0777)
			file.Seek(int64(indexEntry.Offset), 0)
			dataEntry.Tombstone = 1

			// CRC 4 bajta
			binary.Write(file, binary.LittleEndian, dataEntry.CRC)
			// Timestamp 64 bajta
			binary.Write(file, binary.LittleEndian, dataEntry.Timestamp)
			// Tombstone 1 bajt
			binary.Write(file, binary.LittleEndian, dataEntry.Tombstone)
			//	Keysize 8 bajta
			binary.Write(file, binary.LittleEndian, dataEntry.KeySize)
			//	ValueSize 8 bajta
			binary.Write(file, binary.LittleEndian, dataEntry.ValueSize)
			//	Key KeySize bajta
			binary.Write(file, binary.LittleEndian, dataEntry.key)
			//	Value ValueSize bajta
			binary.Write(file, binary.LittleEndian, dataEntry.value)

			engine.wal.delete(key)
			return true

		}
	}
	return false

}

func (engine *Engine) ForceFlush() {
	engine.memTable.flush()
}

func EngineInit() *Engine {
	engine := Engine{}
	engine.tokenBucket = NewTokenBucket()
	engine.memTable = NewMemTable()
	engine.wal = NewWal()
	engine.lsm = NewLSM()
	engine.cache = NewCache(Config.CacheSize)
	engine.hll = NewHLL(0)
	engine.cms = NewCountMinSketch(0, 0)
	return &engine
}
