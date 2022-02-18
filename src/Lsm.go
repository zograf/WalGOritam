package src

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"fmt"
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

func (l *LSM) GetDataIndexSummary(level int) ([]string, []string, []string, []string, []string) {
	files, _ := ioutil.ReadDir("res" + string(filepath.Separator))
	currentData := make([]string, 0)
	currentIndex := make([]string, 0)
	currentSummary := make([]string, 0)
	currentFilter := make([]string, 0)
	currentToc := make([]string, 0)
	for _, entry := range files {
		if strings.Contains(entry.Name(), "TOC") {
			tokens := strings.Split(entry.Name(), "-")
			if tokens[1] != strconv.Itoa(level) {
				continue
			}
			f, _ := os.Open("res" + string(filepath.Separator) + entry.Name())
			scanner := bufio.NewScanner(f)
			scanner.Scan()
			data := scanner.Text()
			scanner.Scan()
			index := scanner.Text()
			scanner.Scan()
			summary := scanner.Text()
			scanner.Scan()
			filter := scanner.Text()
			currentData = append(currentData, data)
			currentIndex = append(currentIndex, index)
			currentSummary = append(currentSummary, summary)
			currentFilter = append(currentFilter, filter)
			currentToc = append(currentToc, entry.Name())
			f.Close()
		}
	}
	return currentData, currentIndex, currentSummary, currentFilter, currentToc
}

func (l *LSM) Check() (bool, int) {
	for i := range l.LevelsMax {
		data, _, _, _, _ := l.GetDataIndexSummary(i + 1)
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
	sumFirst, sumSecond, filFirst, filSecond string, level int) {

	levelStr := strconv.Itoa(level)
	nowStr := strconv.FormatInt(time.Now().UnixMicro(), 10)
	tablePath := "res" + string(filepath.Separator) + "L-" + levelStr + "-" + nowStr + "Data.bin"
	indexPath := "res" + string(filepath.Separator) + "L-" + levelStr + "-" + nowStr + "Index.bin"
	table, _ := os.OpenFile(tablePath, os.O_CREATE|os.O_RDWR, 0644)
	index, _ := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	indexFirst, _ := os.Open("res" + string(filepath.Separator) + indFirst)
	indexSecond, _ := os.Open("res" + string(filepath.Separator) + indSecond)
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
	var itEntry *IndexEntry
	if !itFirst.HasNext() && itSecond.HasNext() {
		// First remained
		flag := false
		flag2 := false
		for itSecond.HasNext() {
			if first.Key < second.Key {
				entry = ReadDataRow(dataFirst, first.Offset)
				l.processEntry(entry, table, index)
				flag = true
				break
			} else if first.Key == second.Key {
				firstE := ReadDataRow(dataFirst, first.Offset)
				secondE := ReadDataRow(dataSecond, second.Offset)
				if firstE.Timestamp > secondE.Timestamp {
					entry := ReadDataRow(dataFirst, first.Offset)
					l.processEntry(entry, table, index)
					flag = true
					flag2 = true
					break
				} else {
					flag = true
					break
				}
			}
			itEntry = itSecond.GetNext()
			entry = ReadDataRow(dataSecond, itEntry.Offset)
			l.processEntry(entry, table, index)
		}
		if flag {
			if !flag2 {
				entry = ReadDataRow(dataSecond, itEntry.Offset)
				l.processEntry(entry, table, index)
			}
			for itSecond.HasNext() {
				itEntry := itSecond.GetNext()
				entry = ReadDataRow(dataSecond, itEntry.Offset)
				l.processEntry(entry, table, index)
			}
		} else {
			entry = ReadDataRow(dataFirst, first.Offset)
			l.processEntry(entry, table, index)
		}
	} else if !itSecond.HasNext() && itFirst.HasNext() {
		// Second remained
		flag := false
		flag2 := false
		for itFirst.HasNext() {
			if second.Key < first.Key {
				entry = ReadDataRow(dataSecond, second.Offset)
				l.processEntry(entry, table, index)
				flag = true
				break
			} else if first.Key == second.Key {
				firstE := ReadDataRow(dataFirst, first.Offset)
				secondE := ReadDataRow(dataSecond, second.Offset)
				if firstE.Timestamp > secondE.Timestamp {
					flag = true
					break
				} else {
					entry := ReadDataRow(dataSecond, second.Offset)
					l.processEntry(entry, table, index)
					flag2 = true
					flag = true
					break
				}
			}
			itEntry := itFirst.GetNext()
			entry = ReadDataRow(dataFirst, itEntry.Offset)
			l.processEntry(entry, table, index)
		}
		if flag {
			if flag2 {
				itEntry := itFirst.GetNext()
				entry = ReadDataRow(dataFirst, itEntry.Offset)
			}
			for itFirst.HasNext() {
				itEntry := itFirst.GetNext()
				entry = ReadDataRow(dataFirst, itEntry.Offset)
				l.processEntry(entry, table, index)
			}
		} else {
			entry = ReadDataRow(dataSecond, second.Offset)
			l.processEntry(entry, table, index)
		}
	} else {
		// Both ran out
		if first.Key < second.Key {
			entry = ReadDataRow(dataFirst, first.Offset)
			l.processEntry(entry, table, index)
			entry = ReadDataRow(dataSecond, second.Offset)
			l.processEntry(entry, table, index)
		} else {
			entry = ReadDataRow(dataSecond, second.Offset)
			l.processEntry(entry, table, index)
			entry = ReadDataRow(dataFirst, first.Offset)
			l.processEntry(entry, table, index)
		}
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
	os.Remove("res" + string(filepath.Separator) + filFirst)
	os.Remove("res" + string(filepath.Separator) + filSecond)
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
		data, index, summary, filter, toc := l.GetDataIndexSummary(level + 1)
		for {
			dataFirst, dataSecond := data[0], data[1]
			indFirst, indSecond := index[0], index[1]
			sumFirst, sumSecond := summary[0], summary[1]
			filFirst, filSecond := filter[0], filter[1]
			fmt.Println(filFirst)
			fmt.Println(filSecond)
			tocFirst, tocSecond := toc[0], toc[1]
			data = data[2:]
			index = index[2:]
			summary = summary[2:]
			filter = filter[2:]
			toc = toc[2:]
			// Compress and retreive dataPath, indexPath
			l.Compress(dataFirst, dataSecond, indFirst, indSecond, sumFirst, sumSecond, filFirst, filSecond, level+2)
			os.Remove("res" + string(filepath.Separator) + tocFirst)
			os.Remove("res" + string(filepath.Separator) + tocSecond)
			// Exit condition (no more compacting on this level)
			if len(data) <= 1 {
				break
			}
		}
	}
}
