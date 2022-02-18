package src

import (
	"fmt"
	"strings"
)

type Engine struct {
	tokenBucket *TokenBucket
	wal         *Wal
	memTable    *Memtable
	lsm         *LSM
	cache 		*Cache
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
	flag := engine.memTable.Set(key, byteValue)
	if flag {
		engine.lsm.Run()
	}
	fmt.Println("SUCCESS! Key-Value pair { " + key + " : " + value + " }")
}

func (engine *Engine) EngineGet(key string) []byte{
	fmt.Println("GET")
	val := engine.memTable.Get(key)
	if val != nil{
		return val
	}
	val = engine.cache.Search(key)
	if val != nil{
		engine.cache.Put(key, val)
		return val
	}
	var currentData []string
	var currentIndex []string
	var currentSummary []string
	var currentFilter []string
	var currentToc []string
	var currentMetadata []string
	fmt.Println(currentData, currentIndex, currentSummary, currentFilter, currentToc, currentMetadata)
	//filesByLevels := make([][]string, Config.LsmMaxLevels)
	var level int
	fmt.Println(level)
	for i := 1; i < Config.LsmMaxLevels; i++{
		currentData, currentIndex, currentSummary, currentFilter, currentToc, currentMetadata = engine.lsm.GetDataIndexSummary(i)

	}
	return nil

}

func (engine *Engine) EngineDelete(key string) {
	fmt.Println("DEL")
}

func (engine *Engine) ForceFlush() {
	engine.memTable.flush()
}

func EngineInit() *Engine {

	engine := Engine{}
	engine.tokenBucket = NewTokenBucket()
	engine.memTable = NewMemTable()
	engine.wal = NewWal()
	max := []uint8{6, 6, 6}
	req := []uint8{2, 2, 2}
	engine.lsm = NewLSM(max, req)
	return &engine
}
