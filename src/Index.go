package src

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

func WriteIndexRow(key []byte, keySize uint8, offset uint32, indexF *os.File) {
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

type IndexEntry struct {
	KeySize uint8
	Key     string
	Offset  uint32
}

type IndexIterator struct {
	file *os.File
}

func (mti *IndexIterator) HasNext() bool {
	temp := make([]byte, 1)
	_, err := mti.file.Read(temp)

	// return to position before function call
	mti.file.Seek(-1, 1)
	if err != nil {
		if err == io.EOF {
			return false
		} else {
			return true
		}
	}
	return true
}

func (mti *IndexIterator) GetNext() *IndexEntry {
	if mti.HasNext() {
		temp := make([]byte, 1)
		_, err := mti.file.Read(temp)
		if err != nil {
			panic(err)
		}
		KeySize := temp[0]
		//fmt.Println("Key size")
		//fmt.Println(KeySize)

		data1 := make([]byte, KeySize)
		_, err = mti.file.Read(data1)
		if err != nil {
			panic(err)
		}
		Key := string(data1[:])
		//fmt.Println("key")
		//fmt.Println(Key)

		data2 := make([]byte, 4)
		_, err = mti.file.Read(data2)
		if err != nil {
			panic(err)
		}
		Offset := binary.LittleEndian.Uint32(data2)
		//fmt.Println("Offset")
		//fmt.Println(Offset)
		//fmt.Println("-------------------------------------------")
		return &IndexEntry{
			KeySize: KeySize,
			Key:     Key,
			Offset:  Offset,
		}
	}
	return nil
}

func ReadIndex(name string) {
	fl, _ := os.OpenFile("res"+string(filepath.Separator)+name, os.O_RDWR, 0777)
	defer fl.Close()

	it := IndexIterator{file: fl}
	for it.HasNext() {
		it.GetNext()
	}
}
