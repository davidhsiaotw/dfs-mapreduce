//go:build top5_ips

package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Item struct {
	Key   string
	Count int
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <directory>\n", os.Args[0])
		os.Exit(1)
	}

	counts := make(map[string]int)

	dir := os.Args[1]
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading directory: %v\n", err)
		os.Exit(1)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := filepath.Join(dir, entry.Name())
		file, err := os.Open(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file %s: %v\n", filename, err)
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				key := fields[0]
				countStr := fields[len(fields)-1]
				count, err := strconv.Atoi(countStr)
				if err == nil {
					counts[key] += count
				}
			}
		}
		file.Close()

		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error reading file %s: %v\n", filename, err)
		}
	}

	var items []Item
	for k, v := range counts {
		items = append(items, Item{Key: k, Count: v})
	}

	// Sort items descending by count
	sort.Slice(items, func(i, j int) bool {
		return items[i].Count > items[j].Count
	})

	limit := 5
	if limit > len(items) {
		limit = len(items)
	}

	for i := 0; i < limit; i++ {
		fmt.Printf("%s: %d\n", items[i].Key, items[i].Count)
	}
}