package src

import (
	"encoding/binary"
	_ "fmt"
	_ "hash"
	_ "math"
	_ "sort"
	"time"
	_ "time"
	_ "unsafe"
)

type WalEntry struct {
	Crc       uint32
	Seed      uint64
	Tombstone byte //deleted = 1
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
}

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE
)

//func CRC32(data []byte) uint32 {
//	return crc32.ChecksumIEEE(data)
//}

func newWalEntry() *WalEntry {
	entry := WalEntry{
		Crc:       0,
		Seed:      uint64(time.Now().Unix()),
		Tombstone: 0, //1 if deleted
		KeySize:   0,
		ValueSize: 0,
		Key:       nil,
		Value:     nil,
	}
	return &entry
}

func (walEntry *WalEntry) put(key string, value []byte) {
	walEntry.Key = []byte(key)
	walEntry.Value = value
	walEntry.KeySize = uint32(len(walEntry.Key))
	walEntry.ValueSize = uint32(len(walEntry.Value))
}

func (walEntry *WalEntry) prepareDump() {
	walEntry.Crc = CRC32(walEntry.toBytes())
}

func (walEntry *WalEntry) checkValidity() bool { // if valid returns true
	readCrc := walEntry.Crc
	walEntry.Crc = 0
	walEntry.prepareDump()
	return readCrc == walEntry.Crc
}

func (walEntry *WalEntry) toBytes() []byte {
	allBytes := make([]byte, 0)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, walEntry.Crc)
	allBytes = append(allBytes, bs...)

	bs = make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, walEntry.Seed)
	allBytes = append(allBytes, bs...)

	allBytes = append(allBytes, walEntry.Tombstone)

	bs = make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, walEntry.KeySize)
	allBytes = append(allBytes, bs...)

	bs = make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, walEntry.ValueSize)
	allBytes = append(allBytes, bs...)

	allBytes = append(allBytes, walEntry.Key...)
	allBytes = append(allBytes, walEntry.Value...)

	return allBytes
}

func main() {
	WalTest()
}
