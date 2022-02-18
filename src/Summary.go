package src

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const BLOCKSIZE = 16

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
	for iter.HasNext() {
		currentEntry = iter.GetNext()
		bloom.Add(currentEntry.Key)
		if i%BLOCKSIZE == 0 {
			sampleKeys = append(sampleKeys, currentEntry)
		}
		i++

		dataEntry = ReadDataRow(dataFileName, currentEntry.Offset)
		data = append(data, dataEntry.value)
	}
	EncodeBloomFilter(bloom, nameFilter)
	merkle := FormMerkle(data)
	merkle.WriteMetadata(strings.Replace(indexFile.Name(), "Index.bin", "Metadata.txt", 1))

	summaryFile, _ := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
	defer summaryFile.Close()

	var offset uint32
	offset = 0
	last := sampleKeys[len(sampleKeys)-1]
	WriteIndexRow([]byte(last.Key), last.KeySize, offset, summaryFile)
	offset += uint32(last.KeySize) + 5

	for _, key := range sampleKeys {
		WriteIndexRow([]byte(key.Key), key.KeySize, offset, summaryFile)
		offset += uint32(last.KeySize) + 5
	}
	nowStr := strings.Replace(indexFile.Name(), "Index.bin", "", 1)
	FormToc(nowStr)

}
func Search(key string, summaryFile *os.File) (uint32, error) {
	iter := IndexIterator{summaryFile}
	last := iter.GetNext()
	first := iter.GetNext()

	if key < first.Key[:] || key > last.Key {
		return 0, errors.New("Not in this SStable")
	}
	previous := first

	for iter.HasNext() {
		current := iter.GetNext()
		if key >= previous.Key && key < current.Key {
			break
		}
		previous = current
	}
	return previous.Offset, nil
}
