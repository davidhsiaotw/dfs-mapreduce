package main

import (
	"bytes"
	"fmt"
	"mapreduce/util"
)

// Map emits <URL, "1"> for every log line
func Map(line []byte) []util.KeyValue {
	parts := bytes.Split(line, []byte("\t"))
	if len(parts) >= 4 {
		url := parts[3]
		return []util.KeyValue{{Key: url, Value: []byte("1")}}
	}
	return nil
}

// Reduce sums the counts
func Reduce(key []byte, values [][]byte) []byte {
	return []byte(fmt.Sprintf("%d", len(values)))
}
