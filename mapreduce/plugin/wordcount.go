package main

import (
	"bytes"
	"mapreduce/util"
	"regexp"
	"strconv"
)

// Map splits the line into words and emits a <word, "1"> pair for each.
func Map(line []byte) []util.KeyValue {
	lowerCaseLine := bytes.ToLower(line)
	var wordRegex = regexp.MustCompile(`\w+`)
	words := wordRegex.FindAll(lowerCaseLine, -1)
	
	kvs := make([]util.KeyValue, 0, len(words))
	for _, word := range words {
		if len(word) > 0 {
			kvs = append(kvs, util.KeyValue{Key: word, Value: []byte("1")})
		}
	}
	return kvs
}

// Reduce receives a list of all values associated with that key (all "1"s).
func Reduce(key []byte, values [][]byte) []byte {
	return []byte(strconv.Itoa(len(values)))
}
