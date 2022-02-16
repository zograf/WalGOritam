package src

import (
	"encoding/binary"
	"encoding/gob"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type LSM struct {
	LevelsMax      []uint8
	LevelsRequired []uint8
}

func NewLSM(max, req []uint8) *LSM {
	l := LSM{}
	l.LevelsMax = max
	l.LevelsRequired = req
	return &l
}

// Serializing
func EncodeLSM(l *LSM, path string) {
	file, err := os.Create(path)
	check(err)
	encoder := gob.NewEncoder(file)
	encoder.Encode(l)
	file.Close()
}

// Deserializing
func DecodeLSM(path string) *LSM {
	file, err := os.Open(path)
	check(err)
	decoder := gob.NewDecoder(file)
	var l LSM
	err = decoder.Decode(&l)
	file.Close()
	return &l
}

func (l *LSM) GetDataIndexSummary(level int) ([]string, []string, []string) {
	files, _ := ioutil.ReadDir("./res/")
	currentData := make([]string, 0)
	currentIndex := make([]string, 0)
	currentSummary := make([]string, 0)
	for _, entry := range files {
		split := strings.Split(entry.Name(), "-")
		intLevel, _ := strconv.Atoi(split[1])
		if intLevel == level {
			if strings.Contains(entry.Name(), "Index") {
				currentIndex = append(currentIndex, entry.Name())
			} else if strings.Contains(entry.Name(), "Summary") {
				currentSummary = append(currentSummary, entry.Name())
			} else {
				currentData = append(currentData, entry.Name())
			}
		}
	}
	return currentData, currentIndex, currentSummary
}

func (l *LSM) Check() (bool, int) {
	for i := range l.LevelsMax {
		data, _, _ := l.GetDataIndexSummary(i + 1)
		max := l.LevelsMax[i]
		req := l.LevelsRequired[i]
		// If size of data reached max or required threshold
		if len(data) == int(max) {
			return true, i
		} else if len(data) >= int(req) {
			return true, i
		}
	}
	return false, 0
}

// Compressing 2 SSTables
func (l *LSM) Compress(
	dataFirst, dataSecond, indFirst, indSecond,
	sumFirst, sumSecond string, level int) {

	levelStr := strconv.Itoa(level)
	nowStr := strconv.FormatInt(time.Now().UnixMicro(), 10)
	tablePath := "res" + string(filepath.Separator) + "L-" + levelStr + "-" + nowStr + ".bin"
	indexPath := "res" + string(filepath.Separator) + "L-" + levelStr + "-" + nowStr + "Index.bin"
	table, _ := os.OpenFile(tablePath, os.O_CREATE|os.O_RDWR, 0644)
	index, _ := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	indexFirst, _ := os.Open("res" + string(filepath.Separator) + indFirst)
	indexSecond, _ := os.Open("res" + string(filepath.Separator) + indSecond)
	indexFirst.Seek(0, 0)
	indexSecond.Seek(0, 0)
	itFirst := IndexIterator{file: indexFirst}
	itSecond := IndexIterator{file: indexSecond}
	first := itFirst.GetNext()
	second := itSecond.GetNext()
	var entry Entry
	for {
		if !itFirst.HasNext() || !itSecond.HasNext() {
			break
		}
		// Check timestamps
		firstEntry := ReadDataRow(dataFirst, first.Offset)
		secondEntry := ReadDataRow(dataSecond, second.Offset)
		if first.Key == second.Key {
			// Choose larger
			if firstEntry.Timestamp > secondEntry.Timestamp {
				entry = firstEntry
			} else {
				entry = secondEntry
			}
			first = itFirst.GetNext()
			second = itSecond.GetNext()
		} else if first.Key < second.Key {
			entry = firstEntry
			first = itFirst.GetNext()
		} else {
			entry = secondEntry
			second = itSecond.GetNext()
		}
		l.processEntry(entry, table, index)
	}
	// If first one ran out
	if !itFirst.HasNext() && itSecond.HasNext() {
		// First remained
		entry = ReadDataRow(dataFirst, first.Offset)
		l.processEntry(entry, table, index)
		for itSecond.HasNext() {
			itEntry := itSecond.GetNext()
			entry = ReadDataRow(dataSecond, itEntry.Offset)
			l.processEntry(entry, table, index)
		}
	} else if !itSecond.HasNext() && itFirst.HasNext() {
		// Second remained
		entry = ReadDataRow(dataSecond, second.Offset)
		l.processEntry(entry, table, index)
		for itFirst.HasNext() {
			itEntry := itFirst.GetNext()
			entry = ReadDataRow(dataFirst, itEntry.Offset)
			l.processEntry(entry, table, index)
		}
	} else {
		entry = ReadDataRow(dataFirst, first.Offset)
		l.processEntry(entry, table, index)
		entry = ReadDataRow(dataSecond, second.Offset)
		l.processEntry(entry, table, index)
	}
	GenerateSummary(index)
	// Close and remove excess
	table.Close()
	index.Close()
	os.Remove("res" + string(filepath.Separator) + dataFirst)
	os.Remove("res" + string(filepath.Separator) + dataSecond)
	os.Remove("res" + string(filepath.Separator) + indFirst)
	os.Remove("res" + string(filepath.Separator) + indSecond)
	os.Remove("res" + string(filepath.Separator) + sumFirst)
	os.Remove("res" + string(filepath.Separator) + sumSecond)
}

func (l *LSM) processEntry(entry Entry, table, index *os.File) {
	// If it's deleted
	if entry.Tombstone == 1 {
		return
	}
	temp, _ := table.Seek(0, io.SeekCurrent)
	// CRC 4 bajta
	binary.Write(table, binary.LittleEndian, entry.CRC)
	// Timestamp 64 bajta
	binary.Write(table, binary.LittleEndian, entry.Timestamp)
	// Tombstone 1 bajt
	binary.Write(table, binary.LittleEndian, entry.Tombstone)
	//	Keysize 8 bajta
	binary.Write(table, binary.LittleEndian, entry.KeySize)
	//	ValueSize 8 bajta
	binary.Write(table, binary.LittleEndian, entry.ValueSize)
	//	Key KeySize bajta
	binary.Write(table, binary.LittleEndian, []byte(entry.key))
	//	Value ValueSize bajta
	binary.Write(table, binary.LittleEndian, entry.value)

	offset := uint32(temp)
	WriteIndexRow([]byte(entry.key), entry.KeySize, offset, index)
}

func (l *LSM) Run() {
	for {
		ok, level := l.Check()
		if !ok {
			break
		}
		data, index, summary := l.GetDataIndexSummary(level + 1)
		for {
			dataFirst, dataSecond := data[0], data[1]
			indFirst, indSecond := index[0], index[1]
			sumFirst, sumSecond := summary[0], summary[1]
			data = data[2:]
			index = index[2:]
			summary = summary[2:]
			// Compress and retreive dataPath, indexPath
			l.Compress(dataFirst, dataSecond, indFirst, indSecond, sumFirst, sumSecond, level+2)
			// Exit condition (no more compacting on this level)
			if len(data) <= 1 {
				break
			}
		}
	}
}
