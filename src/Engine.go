package src

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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

func (engine *Engine) ProcessRequest(tokens []string) (error, []byte) {
	if tokens[0] == "GET_TOTAL_KEYS" {
		if len(tokens) == 1 {
			value := engine.hll.Estimate()
			fmt.Println(value)
			return nil, nil
		} else {
			return errors.New("Invalid key"), nil
		}
	} else if tokens[0] == "GET_REQ_PER_KEY" {
		if len(tokens) == 2 {
			value := engine.cms.Find(tokens[1])
			fmt.Println(value)
			return nil, nil
		} else {
			return errors.New("Invalid key"), nil
		}
	}
	if strings.Compare(tokens[1], "inf") == 0 || strings.Compare(tokens[1], "-inf") == 0 {
		return errors.New("Invalid key"), nil
	}
	err := engine.tokenBucket.CheckBucket()
	if err != nil {
		fmt.Println("There are no more tokens left")
		return err, nil
	}
	if tokens[0] == "PUT" {
		if len(tokens) == 3 {
			engine.EnginePut(tokens[1], tokens[2])
		} else {
			return errors.New("Invalid input format"), nil
		}
	} else if tokens[0] == "GET" {
		if len(tokens) == 2 {
			val, found := engine.EngineGet(tokens[1])
			if !found {
				fmt.Println("Key not found")
				return nil, nil
			}
			return nil, val
		} else {
			return errors.New("Invalid input format"), nil
		}
	} else if tokens[0] == "DEL" {
		if len(tokens) == 2 {
			engine.EngineDelete(tokens[1])
			return nil, nil
		} else {
			return errors.New("Invalid input format"), nil
		}
	} else if tokens[0] == "PUT_HLL" {
		if len(tokens) == 3 {
			value, err := strconv.ParseUint(tokens[2], 10, 8)
			if err != nil {
				return err, nil
			}
			hll := NewHLL(uint8(value))
			byteArray := HLLToByteArray(hll)
			engine.EnginePutHLLCMS(tokens[1], byteArray)
			return nil, nil
		}
	} else if tokens[0] == "PUT_CMS" {
		if len(tokens) == 4 {
			epsilon, err := strconv.ParseFloat(tokens[2], 64)
			if err != nil {
				return err, nil
			}
			delta, err := strconv.ParseFloat(tokens[3], 64)
			if err != nil {
				return err, nil
			}
			cms := NewCountMinSketch(epsilon, delta)
			byteArray := CountMinSketchToByteArray(cms)
			engine.EnginePutHLLCMS(tokens[1], byteArray)
			return nil, nil
		}

	}
	return errors.New("Invalid input format"), nil
}

func (engine *Engine) EnginePutHLLCMS(key string, value []byte) {
	engine.wal.put(key, value)
	engine.wal.deleteSegments()
	engine.hll.Add(key)
	engine.cms.Add(key)
	flag := engine.memTable.Set(key, value, 0)
	if flag {
		engine.lsm.Run()
	}
}

func (engine *Engine) EnginePut(key, value string) {
	byteValue := []byte(value)
	engine.wal.put(key, byteValue)
	engine.wal.deleteSegments()
	engine.hll.Add(key)
	engine.cms.Add(key)
	flag := engine.memTable.Set(key, byteValue, 0)
	if flag {
		engine.lsm.Run()
	}
}

func (engine *Engine) EngineGet(key string) ([]byte, bool) {
	fmt.Println("GET")
	val, deleted := engine.memTable.Get(key)
	if !deleted {
		fmt.Println("Value deleted")
		return nil, false
	}

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
				if dataEntry.key == key && dataEntry.Timestamp > biggestTimestampEntry.Timestamp {
					biggestTimestampEntry = dataEntry
					found = true
				}
			}
			indexFile.Close()
		}
		// if it is found on lower level there is no need to look on upper ones
		if found{
			break
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

func (engine *Engine) EngineDelete(key string) {
	engine.wal.delete(key)
	engine.cache.DeleteElement(key)
	engine.memTable.Delete(key)
	engine.cms.Remove(key)
}

func (engine *Engine) ForceFlush() {
	if engine.memTable.size > 0 {
		engine.memTable.flush()
		engine.lsm.Run()
	}

}

func EngineInit() *Engine {
	if _, err := os.Stat("res"); os.IsNotExist(err) {
		os.Mkdir("res", 0777)
	}
	if _, err := os.Stat("wal"); os.IsNotExist(err) {
		os.Mkdir("wal", 0777)
	}
	engine := Engine{}
	engine.tokenBucket = NewTokenBucket()
	engine.memTable = NewMemTable()
	engine.wal = NewWal()
	engine.lsm = NewLSM()
	engine.cache = NewCache(Config.CacheSize)
	if _, err := os.Stat("res" + string(filepath.Separator) + "hll.gob"); os.IsNotExist(err) {
		engine.hll = NewHLL(0)
	} else {
		engine.hll = DecodeHLL("res" + string(filepath.Separator) + "hll.gob")
	}
	if _, err := os.Stat("res" + string(filepath.Separator) + "cms.gob"); os.IsNotExist(err) {
		engine.cms = NewCountMinSketch(0, 0)
	} else {
		engine.cms = DecodeCountMinSketch("res" + string(filepath.Separator) + "cms.gob")
	}
	return &engine
}
