package main

import (
	"bytes"
	"fmt"
	"mapreduce/util"
	"net/url"
)

// Map extracts the domain from the URL and emits <domain, "1">
func Map(line []byte) []util.KeyValue {
	parts := bytes.Fields(line)
	if len(parts) >= 4 {
		rawUrl := string(parts[3])

		u, err := url.Parse(rawUrl)
        if err != nil {
            return nil
        }

        domain := u.Hostname() 

        if domain != "" {
            return []util.KeyValue{{Key: []byte(domain), Value: []byte("1")}}
        }
	}
	return nil
}

// Reduce sums the counts to find unique domains
func Reduce(key []byte, values [][]byte) []byte {
	return []byte(fmt.Sprintf("%d", len(values)))
}
