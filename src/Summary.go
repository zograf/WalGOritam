package src

import (
	"errors"
	"os"
	"strings"
)

const BLOCKSIZE = 16

//write last then normal input
func GenerateSummary(indexFile *os.File) {
	name := strings.Replace(indexFile.Name(), "Index", "Summary", 1)
	iter := IndexIterator{indexFile}

	var currentEntry *IndexEntry

	sampleKeys := make([]*IndexEntry, 0, 0)

	//summaryFile, _ := os.Create(name)

	i := 0
	for iter.HasNext() {
		currentEntry = iter.GetNext()
		if i%BLOCKSIZE == 0 {
			sampleKeys = append(sampleKeys, currentEntry)
		}
		i++
	}

	summaryFile, _ := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 777)
	defer summaryFile.Close()

	var offset uint16
	offset = 0
	last := sampleKeys[len(sampleKeys)-1]
	WriteIndexRow([]byte(last.Key), last.KeySize, offset, summaryFile)
	offset += uint16(last.KeySize) + 3

	for _, key := range sampleKeys {
		WriteIndexRow([]byte(key.Key), key.KeySize, offset, summaryFile)
		offset += uint16(last.KeySize) + 3
	}

}
func Search(key string, summaryFile *os.File) (uint16, error) {
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
