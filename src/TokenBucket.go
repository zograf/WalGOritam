package src

import (
	"encoding/gob"
	"errors"
	"os"
	"time"
)

// MAX_BUCKET - Max amount of tokens
// INTERVAL   - Reset time (in seconds)

type TokenBucket struct {
	Bucket    int64
	Interval  int64
	Timestamp int64
}

func NewTokenBucket() *TokenBucket {
	tb := TokenBucket{}
	tb.Bucket = Config.TokenBucketMax
	tb.Interval = Config.TokenBucketInterval
	tb.Timestamp = time.Now().Unix()
	return &tb
}

func (tb *TokenBucket) checkTimeStamp() bool {
	// Check if tb needs to be reset
	if time.Now().Unix()-tb.Timestamp >= tb.Interval {
		return true
	}
	return false
}

func (tb *TokenBucket) CheckBucket() error {
	// Check if timestamp needs to be updated
	if tb.checkTimeStamp() {
		tb.Bucket = Config.TokenBucketMax
		tb.Timestamp = time.Now().Unix()
	}
	if tb.Bucket == 0 {
		return errors.New("There are no more tokens left")
	}
	tb.Bucket--
	return nil
}

// Encode/Decode for TokenBucket
// is probably unneccessary
func EncodeTokenBucket(tb *TokenBucket, path string) {
	file, err := os.Create(path)
	check(err)
	encoder := gob.NewEncoder(file)
	encoder.Encode(tb)
	file.Close()
}

func DecodeTokenBucket(path string) *TokenBucket {
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
