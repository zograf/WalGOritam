package src

import "fmt"

type Engine struct {
	tokenBucket *TokenBucket
	wal         *Wal
	memTable    *Memtable
	lsm         *LSM
}

func (engine *Engine) EnginePut(key, value string) {
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

func (engine *Engine) EngineGet(key string) {
	fmt.Println("GET")
}

func (engine *Engine) EngineDelete(key string) {
	fmt.Println("DEL")
}

func EngineInit() *Engine {
	engine := Engine{}
	engine.tokenBucket = NewTokenBucket()
	engine.memTable = NewMemTable()
	engine.wal = NewWal()
	max := []uint8{5, 5, 5}
	req := []uint8{2, 2, 2}
	engine.lsm = NewLSM(max, req)
	return &engine
}
