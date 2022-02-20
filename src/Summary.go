package src

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const BLOCKSIZE = 20

//write last then normal input
func GenerateSummary(indexFile *os.File) {
	name := strings.Replace(indexFile.Name(), "Index", "Summary", 1)
	nameFilter := strings.Replace(indexFile.Name(), "Index", "Filter", 1)

	lvl_tokens := strings.Split(nameFilter, "-")

	level, _ := strconv.Atoi(lvl_tokens[1])
	bloom := NewBloomFilter(Config.BloomFilterExpectedElementsL1[level-1], Config.BloomFilterFalsePositive)

	indexFile.Seek(0, 0)
	iter := IndexIterator{indexFile}

	var currentEntry *IndexEntry

	sampleKeys := make([]*IndexEntry, 0, 0)

	//summaryFile, _ := os.Create(name)

	data := make([][]byte, 0)
	dataFileName := strings.Replace(indexFile.Name(), "Index", "Data", 1)
	dataFileName = strings.Replace(dataFileName, "res"+string(filepath.Separator), "", 1)
	var dataEntry Entry
	i := 0
	var offset uint32
	offset = 0
	for iter.HasNext() {
		currentEntry = iter.GetNext()
		bloom.Add(currentEntry.Key)
		if i%BLOCKSIZE == 0 {
			//fmt.Println(currentEntry.Offset)
			//fmt.Println(offset)
			copyEntry := IndexEntry{
				KeySize: currentEntry.KeySize,
				Key:     currentEntry.Key,
				Offset:  currentEntry.Offset,
			}
			sampleKeys = append(sampleKeys, &copyEntry)
			sampleKeys[len(sampleKeys)-1].Offset = offset
		}
		//fmt.Println(currentEntry.KeySize)
		offset += uint32(currentEntry.KeySize) + 5
		i++

		dataEntry = ReadDataRow(dataFileName, currentEntry.Offset)
		data = append(data, dataEntry.value)
	}
	if sampleKeys[len(sampleKeys)-1].Key != currentEntry.Key {
		sampleKeys = append(sampleKeys, currentEntry)
	}

	EncodeBloomFilter(bloom, nameFilter)
	merkle := FormMerkle(data)
	merkle.WriteMetadata(strings.Replace(indexFile.Name(), "Index.bin", "Metadata.txt", 1))

	summaryFile, _ := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
	defer summaryFile.Close()

	last := sampleKeys[len(sampleKeys)-1]
	WriteIndexRow([]byte(last.Key), last.KeySize, last.Offset, summaryFile)

	for _, key := range sampleKeys {
		WriteIndexRow([]byte(key.Key), key.KeySize, key.Offset, summaryFile)
	}
	nowStr := strings.Replace(indexFile.Name(), "Index.bin", "", 1)
	FormToc(nowStr)

}
func Search(key string, summaryFile *os.File) (uint32, bool) {
	iter := IndexIterator{summaryFile}
	last := iter.GetNext()
	first := iter.GetNext()

	if key < first.Key[:] || key > last.Key {
		return 0, false
	}
	previous := first

	for iter.HasNext() {
		current := iter.GetNext()
		if key >= previous.Key && key <= current.Key {
			break
		}
		previous = current
	}
	return previous.Offset, true
}
