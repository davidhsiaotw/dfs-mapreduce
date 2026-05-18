package main

import (
	"bytes"
	"fmt"
	"mapreduce/util"
	"unicode"
)

// Map splits the line into words and emits a <word, "1"> pair for each.
func Map(line []byte) []util.KeyValue {
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	words := bytes.FieldsFunc(line, f)
	
	var kvs []util.KeyValue
	for _, word := range words {
		word = bytes.ToLower(word)
		if len(word) > 0 {
			kvs = append(kvs, util.KeyValue{Key: word, Value: []byte("1")})
		}
	}
	return kvs
}

// Reduce receives a list of all values associated with that key (all "1"s).
func Reduce(key []byte, values [][]byte) []byte {
	return []byte(fmt.Sprintf("%d", len(values)))
}
