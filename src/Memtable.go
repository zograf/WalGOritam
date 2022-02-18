package src

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const THRESHOLD = 10000

type Memtable struct {
	threshold uint16
	size      uint16
	sl        *SkipList
}

func (mt *Memtable) Get(key string) []byte {
	return mt.sl.GetVal(key)
}

func (mt *Memtable) Delete(key string) {
	mt.sl.Delete(key)
}

func (mt *Memtable) Set(key string, val []byte) bool {
	flag := false
	if mt.size+32+uint16(len(val)) >= mt.threshold {
		flag = true
		mt.flush()
		sl := MakeSkipList()
		*mt = Memtable{
			threshold: mt.threshold,
			size:      0,
			sl:        &sl,
		}
	}
	mt.size += 32 + uint16(len(val))
	mt.sl.Set(key, val)
	return flag
}

func (mt *Memtable) flush() {
	nowStr := strconv.FormatInt(time.Now().UnixMicro(), 10)
	flPath := "res" + string(filepath.Separator) + "L-1-" + nowStr + "Data.bin"
	fl, err := os.Create(flPath)
	if err != nil {
		panic(err)
	}
	defer fl.Close()
	if err != nil {
		panic(err)
	}

	indexPath := "res" + string(filepath.Separator) + "L-1-" + nowStr + "Index" + ".bin"
	indexF, err := os.Create(indexPath)
	if err != nil {
		panic(err)
	}
	defer indexF.Close()
	if err != nil {
		panic(err)
	}

	// iterating through zero level of skip list
	iterator := mt.sl.CreateIterator()
	var skipNode *SkipListNode

	var CRC uint32
	var Timestamp uint64
	var Tombstone byte
	var ValueSize uint8
	var key []byte
	var KeySize uint8
	var value []byte
	var offset uint32

	indexEntryCount := 0
	for iterator.HasNext() {
		skipNode = iterator.GetNext()

		CRC = CRC32(skipNode.Value)
		Timestamp = uint64(time.Now().Unix())
		Tombstone = 0
		ValueSize = uint8(binary.Size(skipNode.Value))
		key = []byte(skipNode.Key)
		KeySize = uint8(binary.Size(key))
		value = skipNode.Value
		temp, _ := fl.Seek(0, io.SeekCurrent)
		offset = uint32(temp)

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

		WriteIndexRow(key, KeySize, offset, indexF)
		indexEntryCount++
	}
	GenerateSummary(indexF)

}

func FormToc(nowStr string) {
	filePath := nowStr
	file, err := os.Create(filePath + "TOC.txt")
	filePath = strings.ReplaceAll(filePath, "res"+string(filepath.Separator), "")
	if err != nil {
		panic(err)
	}
	_, err = file.WriteString(filePath + "Data.bin\n")
	if err != nil {
		panic(err)
	}
	_, err = file.WriteString(filePath + "Index.bin\n")
	if err != nil {
		panic(err)
	}
	_, err = file.WriteString(filePath + "Summary.bin\n")
	if err != nil {
		panic(err)
	}
	_, err = file.WriteString(filePath + "Filter.gob\n")
	if err != nil {
		panic(err)
	}
	file.Close()
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Entry struct {
	CRC       uint32
	Timestamp uint64
	Tombstone byte
	ValueSize uint8
	key       string
	KeySize   uint8
	value     []byte
}

func ReadDataRow(name string, offset uint32) Entry {
	var CRC uint32
	var Timestamp uint64
	var Tombstone byte
	var ValueSize uint8
	var Key string
	var KeySize uint8
	var value []byte

	file, err := os.OpenFile("res"+string(filepath.Separator)+name, os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		panic(err)
	}

	data1 := make([]byte, 4)
	_, err = file.Read(data1)
	if err != nil {
		panic(err)
	}
	CRC = binary.LittleEndian.Uint32(data1)
	//fmt.Println("CRC:")
	//fmt.Println(CRC)

	data2 := make([]byte, 8)
	_, err = file.Read(data2)
	if err != nil {
		panic(err)
	}
	Timestamp = binary.LittleEndian.Uint64(data2)
	//fmt.Println("timestamp:")
	//fmt.Println(int64(Timestamp))

	data3 := make([]byte, 1)
	_, err = file.Read(data3)
	if err != nil {
		panic(err)
	}
	Tombstone = data3[0]
	//fmt.Println("tombstone:")
	//fmt.Println(Tombstone)

	data4 := make([]byte, 1)
	_, err = file.Read(data4)
	if err != nil {
		panic(err)
	}

	KeySize = data4[0]
	//fmt.Println("Key size")
	//fmt.Println(KeySize)

	data5 := make([]byte, 1)
	_, err = file.Read(data5)
	if err != nil {
		panic(err)
	}
	ValueSize = data5[0]
	//fmt.Println("Value size")
	//fmt.Println(ValueSize)

	data6 := make([]byte, KeySize)
	_, err = file.Read(data6)
	if err != nil {
		panic(err)
	}

	Key = string(data6[:])
	//fmt.Println("key")
	//fmt.Println(Key)

	data7 := make([]byte, ValueSize)
	_, err = file.Read(data7)
	if err != nil {
		panic(err)
	}

	value = data7
	//fmt.Println("val:")
	//fmt.Println(string(data7))

	//fmt.Println("--------------------------------")
	return Entry{
		CRC:       CRC,
		Timestamp: Timestamp,
		Tombstone: Tombstone,
		ValueSize: ValueSize,
		key:       Key,
		KeySize:   KeySize,
		value:     value,
	}
}

// samo za testiranje, puca
func ReadData(name string) {
	for {
		ReadDataRow(name, 0)
	}
}

func NewMemTable() *Memtable {
	sl := MakeSkipList()
	mt := Memtable{
		threshold: uint16(Config.MemtableThreshold),
		size:      0,
		sl:        &sl,
	}
	return &mt
}

func Generate() {
	mt := NewMemTable()

	mt.Set("29", []byte("thrth"))
	mt.Set("21", []byte("dqwd"))
	mt.Set("23", []byte("rgrt"))
	mt.Set("67", []byte("qwd"))
	mt.Set("5657", []byte("ewf"))
	mt.Set("232", []byte("dxwq"))
	mt.Set("98", []byte("rge"))
	mt.Set("222", []byte("nnf"))
	mt.Set("2132", []byte("zxc"))
	mt.Set("9877", []byte("scz"))
	mt.Set("122", []byte("mnh"))
	mt.Set("665", []byte("oip"))
	mt.Set("1211", []byte("bnyy"))
	mt.Set("132", []byte("zzzz"))
	fmt.Println(mt.size)
	var i int32
	for i = 1; i < 100; i++ {
		mt.Set(strconv.Itoa(int(i)), []byte("gfh"))
	}
	mt.Set("73", []byte("asd"))
	mt.Set("27", []byte("pera"))
}
