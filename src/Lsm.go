package src

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type LSM struct {
	Data           map[int][]string
	Indexes        map[int][]string
	LevelsMax      []uint8
	LevelsRequired []uint8
}

func (l *LSM) Check() (bool, int) {
	// LevelsMax and LevelsRequired are the same size
	for i := range l.LevelsMax {
		max := l.LevelsMax[i]
		req := l.LevelsRequired[i]
		data := uint8(len(l.Data[i]))
		// If size of data reached max or required threshold
		if data == max {
			return true, i
		} else if data >= req {
			return true, i
		}
	}
	return false, 0
}

func (l *LSM) Insert(level int, data, index string) {
	l.Data[level] = append(l.Data[level], data)
	l.Indexes[level] = append(l.Indexes[level], index)
}

// Compressing 2 SSTables
func (l *LSM) Compress(dataFirst, dataSecond, indFirst, indSecond string) (string, string) {
	nowStr := strconv.FormatInt(time.Now().UnixMicro(), 10)
	tablePath := "res" + string(filepath.Separator) + nowStr + ".bin"
	indexPath := "res" + string(filepath.Separator) + nowStr + "Index.bin"
	table, _ := os.OpenFile(tablePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	index, _ := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	indexFirst, _ := os.Open(indFirst)
	indexSecond, _ := os.Open(indSecond)
	itFirst := IndexIterator{file: indexFirst}
	itSecond := IndexIterator{file: indexSecond}
	first := itFirst.GetNext()
	second := itSecond.GetNext()
	var entry Entry
	var offset uint16
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
				offset = first.Offset
			} else {
				entry = secondEntry
				offset = first.Offset
			}
		} else if first.Key < second.Key {
			entry = firstEntry
			offset = first.Offset
		} else {
			entry = secondEntry
			offset = second.Offset
		}
		l.processEntry(entry, offset, table, index)
	}
	// If first one ran out
	it := itFirst
	data := dataFirst
	if !itFirst.HasNext() {
		it = itSecond
		data = dataSecond
	}
	for it.HasNext() {
		itEntry := it.GetNext()
		entry := ReadDataRow(data, itEntry.Offset)
		offset := itEntry.Offset
		l.processEntry(entry, offset, table, index)
	}
	// Close and remove excess
	table.Close()
	index.Close()
	os.Remove(dataFirst)
	os.Remove(dataSecond)
	os.Remove(indFirst)
	os.Remove(indSecond)
	return tablePath, indexPath
}

func (l *LSM) processEntry(entry Entry, offset uint16, table, index *os.File) {
	// If it's deleted
	if entry.Tombstone == 1 {
		return
	}
	table.Seek(0, io.SeekEnd)
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

	WriteIndexRow([]byte(entry.key), entry.KeySize, offset, index)
}

func (l *LSM) Run() {
	for {
		ok, level := l.Check()
		if !ok {
			break
		}
		for {
			dataFirst, dataSecond := l.Data[level][0], l.Data[level][1]
			indFirst, indSecond := l.Indexes[level][0], l.Indexes[level][1]
			l.Data[level] = append(l.Data[level][2:])
			l.Indexes[level] = append(l.Indexes[level][2:])
			// Compress and retreive dataPath, indexPath
			data, index := l.Compress(dataFirst, dataSecond, indFirst, indSecond)
			l.Data[level+1] = append(l.Data[level+1], data)
			l.Indexes[level+1] = append(l.Indexes[level+1], index)
			//TODO: Create summary
			// Exit condition (no more compacting on this level)
			if len(l.Data[level]) <= 1 {
				break
			}
		}
	}
}
