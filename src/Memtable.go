package main

import (
	SkipList "Memtable/SkipList"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Memtable struct {
	threshold int
	size int
	sl *SkipList.SkipList

}

func (mt *Memtable) Add(key string, val []byte){
	if mt.size >= mt.threshold{
		mt.flush()
		sl := SkipList.MakeSkipList()
		*mt = Memtable{
			threshold: mt.threshold,
			size: 0,
			sl: &sl,
		}
	}
	mt.size += 64 + len(val)
	mt.sl.Add(key, val)
}

func (mt *Memtable) flush(){
	nowStr := strconv.FormatInt(time.Now().UnixMicro(), 10)
	fl, err := os.Create("res" +  string(filepath.Separator) + nowStr + ".bin")
	defer fl.Close()
	if err != nil {
		panic(err)
	}
	// iteriranje kroz nulti nivo skip liste
	iterator := mt.sl.CreateIterator()
	var skipNode *SkipList.SkipListNode

	for iterator.HasNext(){
		skipNode = iterator.GetNext()

		var CRC uint32 = CRC32(skipNode.Value)
		var Timestamp int64 = time.Now().Unix()
		var Tombstone byte = 0
		var KeySize uint8 = 4
		var ValueSize uint8 = uint8(binary.Size(skipNode.Value))
		var key string = skipNode.Key
		value := skipNode.Value

		// CRC 4 bajta
		binary.Write(fl, binary.LittleEndian, CRC)
		// Timestamp 64 bajta
		binary.Write(fl, binary.LittleEndian, Timestamp)
		// Tombstone 1 bajt
		binary.Write(fl, binary.LittleEndian, Tombstone)
		//	Keysize 8 bajta
		binary.Write(fl, binary.LittleEndian, KeySize)
		//	ValueSize 8 bajta
		binary.Write(fl, binary.LittleEndian, ValueSize)
		//	Key KeySize bajta
		binary.Write(fl, binary.LittleEndian, key)
		//	Value ValueSize bajta
		binary.Write(fl, binary.LittleEndian, value)

		//// CRC 32 bajta
		//bs := make([]byte, 4)
		//binary.LittleEndian.PutUint32(bs, CRC)
		//writer.Write(bs)
		//
		//// Timestamp 64 bajta
		//bs = make([]byte, 64)
		//binary.LittleEndian.PutUint64(bs, uint64(Timestamp))
		//writer.Write(bs)
		//
		//// Tombstone 1 bajt
		//Tombstone := make([]byte, 1)
		//Tombstone[0] = 0
		//writer.Write(Tombstone)
		//
		////	Keysize 8 bajta
		//bs = make([]byte, 8)
		//binary.LittleEndian.PutUint64(bs, uint64(KeySize))
		//writer.Write(bs)
		//
		////	ValueSize 8 bajta
		//bs = make([]byte, 8)
		//binary.LittleEndian.PutUint64(bs, uint64(ValueSize))
		//writer.Write(bs)
		//
		////	Key KeySize bajta
		//bs = make([]byte, KeySize)
		//binary.LittleEndian.PutUint64(bs, uint64(key))
		//writer.Write(bs)
		//
		////	Value ValueSize bajta
		//writer.Write(value)
		//writer.Flush()

	}

}
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}