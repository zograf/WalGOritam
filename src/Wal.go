package src

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

const (
	THRESHOLD_DEFAULT        = 10
	ENTRIES_PER_FILE_DEFAULT = 10
	PATH                     = "wal/WAL"
	LOW_WATERMARK            = 2
)

type Wal struct {
	WalBuffer      []*WalEntry
	threshold      int32
	currentFile    int32
	entriesPerFile int32
	inCurrentFile  int32
	lowWatermark   int
	path           string
}

func NewWal() *Wal {
	threshold := THRESHOLD_DEFAULT
	entriesPerFile := ENTRIES_PER_FILE_DEFAULT
	lowWatermark := LOW_WATERMARK
	path := PATH

	//TODO read config

	thresholdValid := false
	entriesPerFileValid := false
	if thresholdValid {
		fmt.Println("T: Valid")
	}
	if entriesPerFileValid {
		fmt.Println("EPF: Valid")
	}

	files, _ := ioutil.ReadDir("wal/")
	currentFile := len(files)

	return &Wal{
		WalBuffer:      make([]*WalEntry, 0),
		threshold:      int32(threshold),
		currentFile:    int32(currentFile),
		entriesPerFile: int32(entriesPerFile),
		inCurrentFile:  0,
		lowWatermark:   lowWatermark,
		path:           path,
	}

}

func (wal *Wal) put(s string, data []byte) {
	if int32(len(wal.WalBuffer)) >= wal.threshold {
		wal.dump()

	}
	newEntry := newWalEntry()
	newEntry.put(s, data)
	wal.WalBuffer = append(wal.WalBuffer, newEntry)
}

func (wal *Wal) deleteSegments() {
	files, _ := ioutil.ReadDir("wal/")
	fileCount := len(files)

	if fileCount > wal.lowWatermark {
		for _, file := range files {
			os.Remove("wal/" + file.Name())
			fileCount--
			if fileCount == wal.lowWatermark {
				break
			}
		}
		files, _ = ioutil.ReadDir("wal/")

		i := 0
		for _, file := range files {
			os.Rename("wal/"+file.Name(), wal.path+strconv.Itoa(i)+".gob")
			i++
		}
		//i := 0
		//for i = 0; i < fileCount-wal.lowWatermark; i++ {
		//	os.Remove(files[i].Name())
		//
		//}
		//for ; i < fileCount; i++ {
		//	os.Rename(files[i].Name(),
		//		wal.path+strconv.Itoa(i-(fileCount-wal.lowWatermark))+".gob")
		//	//fmt.Println(i - (fileCount - wal.lowWatermark))
		//}
	}

}

func (wal *Wal) dump() bool {
	appendFile, _ := os.OpenFile(PATH+strconv.Itoa(int(wal.currentFile))+".gob", os.O_RDWR|os.O_CREATE, 0644)

	for i := 0; i < len(wal.WalBuffer); i++ {
		wal.WalBuffer[i].prepareDump()
		err := appendToFile(appendFile, wal.WalBuffer[i].toBytes())
		if err != nil {
			return false
		}
		wal.inCurrentFile++
		if wal.inCurrentFile >= wal.entriesPerFile {
			wal.currentFile++
			appendFile, _ = os.OpenFile(PATH+strconv.Itoa(int(wal.currentFile))+".gob", os.O_RDWR|os.O_CREATE, 0644)
		}
	}
	wal.inCurrentFile = 0
	wal.WalBuffer = make([]*WalEntry, 0)
	appendFile.Close()
	return true
}

func appendToFile(file *os.File, data []byte) error {

	//fmt.Println(data)
	file.Seek(0, 2)
	file.Write(data)

	return nil
}

func (wal *Wal) recovery() {

	files, _ := ioutil.ReadDir("wal/")
	fileCount := len(files)

	for i := 0; i < fileCount; i++ {
		fileName := wal.path + strconv.Itoa(i) + ".gob"
		source, _ := os.OpenFile(fileName, os.O_RDONLY, 0777)
		fileL, _ := fileLen(source)
		if fileL == 0 {
			break
		}
		currentFileIndex := 0
		for j := 0; true; j++ {

			bs, err := readRange(source, currentFileIndex, currentFileIndex+21)
			currentFileIndex += 21

			if err == io.EOF {
				source.Close()
				break
			}

			crc := binary.LittleEndian.Uint32(bs[:4])

			seed := binary.LittleEndian.Uint64(bs[4:12])

			tombstone := bs[12:13]

			keySize := binary.LittleEndian.Uint32(bs[13:17])

			valueSize := binary.LittleEndian.Uint32(bs[17:21])

			bs, _ = readRange(source, currentFileIndex, currentFileIndex+int(keySize))
			currentFileIndex += int(keySize)
			key := bs

			bs, _ = readRange(source, currentFileIndex, currentFileIndex+int(valueSize))
			currentFileIndex += int(valueSize)
			value := bs

			currentEntrie := WalEntry{
				Crc:       crc,
				Seed:      seed,
				Tombstone: tombstone[0],
				KeySize:   keySize,
				ValueSize: valueSize,
				Key:       key,
				Value:     value,
			}

			currentEntrie.checkValidity()
		}

	}

}

func readRange(file *os.File, startIndex, endIndex int) ([]byte, error) {
	if startIndex < 0 || endIndex < 0 || startIndex > endIndex {
		return nil, errors.New("indices invalid")
	}

	result := make([]byte, endIndex-startIndex)
	//fmt.Println(endIndex - startIndex)
	_, err := io.ReadFull(file, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func WalTest() {
	wal := NewWal()
	for i := 0; i < 8; i++ {
		for j := 0; j < 10; j++ {
			bs := make([]byte, 4)
			binary.LittleEndian.PutUint32(bs, uint32(j))
			//fmt.Println(bs)
			wal.put(strconv.Itoa(i), bs)
		}
	}
	wal.deleteSegments()
	wal.recovery()
}

//func main() {
//	fmt.Println("asd")
//}
