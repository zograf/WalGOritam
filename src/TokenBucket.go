package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

// MAX_BUCKET - Max amount of tokens
// INTERVAL   - Reset time (in seconds)
const (
	MAX_BUCKET = 4
	INTERVAL   = 5
)

type TokenBucket struct {
	Bucket    uint8
	Timestamp int64
}

func newTokenBucket() *TokenBucket {
	tb := TokenBucket{}
	tb.Bucket = MAX_BUCKET
	tb.Timestamp = time.Now().Unix()
	return &tb
}

func (tb *TokenBucket) checkTimeStamp() bool {
	// Check if tb needs to be reset
	if time.Now().Unix()-tb.Timestamp >= INTERVAL {
		return true
	}
	return false
}

// TODO: Replace prints with actual return errors
func (tb *TokenBucket) process(request uint8) error {
	// Check if timestamp needs to be updated
	if tb.checkTimeStamp() {
		tb.Bucket = MAX_BUCKET
		tb.Timestamp = time.Now().Unix()
	}
	if tb.Bucket == 0 {
		fmt.Println("There are no more tokens left")
	} else {
		tb.Bucket--
		fmt.Println("Request processed successfully")
	}
	return nil
}

// Encode/Decode for TokenBucket
// is probably unneccessary
func encodeTokenBucket(tb *TokenBucket, path string) {
	file, err := os.Create(path)
	check(err)
	encoder := gob.NewEncoder(file)
	encoder.Encode(tb)
	file.Close()
}

func decodeTokenBucket(path string) *TokenBucket {
	file, err := os.Open(path)
	check(err)
	decoder := gob.NewDecoder(file)
	var tb TokenBucket
	err = decoder.Decode(&tb)
	file.Close()
	return &tb
}

/*
func main() {
	tb := newTokenBucket()
	tb.process(5)
	tb.process(5)
	tb.process(5)
	tb.process(5)
	tb.process(5)
	tb.process(5)
	tb.process(5)
	time.Sleep(5 * time.Second)
	tb.process(5)
	tb.process(5)
	tb.process(5)
	tb.process(5)
	tb.process(5)
	tb.process(5)
	tb.process(5)
}
*/