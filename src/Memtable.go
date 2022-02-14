package main

import (
	SkipList "Memtable/SkipList"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Memtable struct {
	threshold uint16
	size uint16
	sl *SkipList.SkipList

}

func (mt *Memtable) Get(key string) []byte{
	return mt.sl.GetVal(key)
}

func (mt *Memtable) Delete(key string){
	mt.sl.Delete(key)
}

func (mt *Memtable) Set(key string, val []byte){
	if mt.size + 32 + uint16(len(val)) >= mt.threshold{
		mt.flush()
		sl := SkipList.MakeSkipList()
		*mt = Memtable{
			threshold: mt.threshold,
			size: 0,
			sl: &sl,
		}
	}
	mt.size += 32 + uint16(len(val))
	mt.sl.Set(key, val)
}

func (mt *Memtable) flush(){
	nowStr := strconv.FormatInt(time.Now().UnixMicro(), 10)
	fl, err := os.Create("res" +  string(filepath.Separator) + nowStr + ".bin")
	if err != nil {
		panic(err)
	}
	defer fl.Close()
	if err != nil {
		panic(err)
	}

	indexF, err := os.Create("res" +  string(filepath.Separator) + nowStr + "Index" + ".bin")
	if err != nil {
		panic(err)
	}
	defer indexF.Close()
	if err != nil {
		panic(err)
	}

	// iterating through zero level of skip list
	iterator := mt.sl.CreateIterator()
	var skipNode *SkipList.SkipListNode

	var CRC uint32
	var Timestamp int64
	var Tombstone byte
	var ValueSize uint8
	var key []byte
	var KeySize uint8
	var value []byte
	var offset uint16

	for iterator.HasNext(){
		skipNode = iterator.GetNext()

		CRC = CRC32(skipNode.Value)
		Timestamp = time.Now().Unix()
		Tombstone = 0
		ValueSize = uint8(binary.Size(skipNode.Value))
		key = []byte(skipNode.Key)
		KeySize = uint8(binary.Size(key))
		value = skipNode.Value
		temp, _ := fl.Seek(0, io.SeekCurrent)
		offset = uint16(temp)

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
	}

}
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func WriteIndexRow(key []byte, keySize uint8, offset uint16, indexF *os.File) {
	err := binary.Write(indexF, binary.LittleEndian, keySize)
	if err != nil {
		panic(err)
	}

	err = binary.Write(indexF, binary.LittleEndian, key)
	if err != nil {
		panic(err)
	}

	err = binary.Write(indexF, binary.LittleEndian, offset)
	if err != nil {
		panic(err)
	}
}



func ReadIndex(name string){
	fl, _ := os.OpenFile("res" + string(filepath.Separator) + name, os.O_RDWR, 0777)
	fl.Seek(0, 0)
	defer fl.Close()

	for {
		temp := make([]byte, 1)
		_, err := fl.Read(temp)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		KeySize := uint8(temp[0])
		fmt.Println("Key size")
		fmt.Println(KeySize)

		Key := make([]byte, KeySize)
		_, err = fl.Read(Key)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		data1 := string(Key[:])
		fmt.Println("key")
		fmt.Println(data1)

		Offset := make([]byte, 2)
		_, err = fl.Read(Offset)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		data := binary.LittleEndian.Uint16(Offset)
		fmt.Println("Offset")
		fmt.Println(data)
	}
}

func Generate(){
	sl := SkipList.MakeSkipList()
	mt := Memtable{
		threshold: 1000,
		size:      0,
		sl:        &sl,
	}

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
	for i= 1; i < 100; i++{
		mt.Set(strconv.Itoa(int(i)), []byte("gfh"))
	}
	mt.Set("73", []byte("asd"))
	mt.Set("27", []byte("pera"))
}

func ReSetata(name string){
	fl, err := os.OpenFile("res" + string(filepath.Separator) + name, os.O_RDWR, 0777)
	fl.Seek(0, 0)
	defer fl.Close()
	fmt.Println("CRC")
	for{
		fmt.Println(fl.Seek(0, io.SeekCurrent))
		CRC := make([]byte, 4)
		_, err = fl.Read(CRC)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		data := binary.LittleEndian.Uint32(CRC)
		fmt.Println(data)

		Timestamp := make([]byte, 8)
		_, err = fl.Read(Timestamp)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		data = binary.LittleEndian.Uint32(Timestamp)
		fmt.Println("timestamp:")
		fmt.Println(int64(data))

		Tombstone := make([]byte, 1)
		_, err = fl.Read(Tombstone)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		fmt.Println("tombstone:")
		fmt.Println(Tombstone)

		temp := make([]byte, 1)
		_, err = fl.Read(temp)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		KeySize := uint8(temp[0])
		fmt.Println("Key size")
		fmt.Println(KeySize)

		temp = make([]byte, 1)
		_, err = fl.Read(temp)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		ValueSize := uint8(temp[0])
		fmt.Println("Value size")
		fmt.Println(ValueSize)

		Key := make([]byte, KeySize)
		_, err = fl.Read(Key)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		data1 := string(Key[:])
		fmt.Println("key")
		fmt.Println(data1)


		Value := make([]byte, ValueSize)
		_, err = fl.Read(Value)
		if err != nil {
			if err == io.EOF{
				break
			}else{
				panic(err)
			}
		}
		fmt.Println("val:")
		fmt.Println(string(Value))
	}
}