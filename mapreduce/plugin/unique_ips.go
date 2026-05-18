package main

import (
	"bytes"
	"fmt"
	"mapreduce/util"
)

// Map emits <IP, "1"> for every log line
func Map(line []byte) []util.KeyValue {
	parts := bytes.Split(line, []byte("\t"))
	if len(parts) >= 3 {
		ip := parts[2]
		return []util.KeyValue{{Key: ip, Value: []byte("1")}}
	}
	return nil
}

// Reduce sums the counts to find unique IPs
func Reduce(key []byte, values [][]byte) []byte {
	return []byte(fmt.Sprintf("%d", len(values)))
}
