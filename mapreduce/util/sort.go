package util

import (
	"bufio"
	"bytes"
	"container/heap"
	"io"
	"os"
)

type KVScanner struct {
	scanner *bufio.Scanner
	Current KeyValue
	valid   bool
}

func NewKVScanner(r io.Reader) *KVScanner {
	return &KVScanner{scanner: bufio.NewScanner(r)}
}

func (s *KVScanner) Next() bool {
	for s.scanner.Scan() {
		line := s.scanner.Bytes()
		parts := bytes.SplitN(line, []byte("\t"), 2)
		if len(parts) == 2 {
			k := make([]byte, len(parts[0]))
			copy(k, parts[0])
			v := make([]byte, len(parts[1]))
			copy(v, parts[1])
			s.Current = KeyValue{Key: k, Value: v}
			s.valid = true
			return true
		}
	}
	s.valid = false
	return false
}

type ScannerHeap []*KVScanner

func (h ScannerHeap) Len() int           { return len(h) }
func (h ScannerHeap) Less(i, j int) bool { return bytes.Compare(h[i].Current.Key, h[j].Current.Key) < 0 }
func (h ScannerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *ScannerHeap) Push(x any) {
	*h = append(*h, x.(*KVScanner))
}
func (h *ScannerHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func ExternalSort(inputFiles []string, outputFile string) error {
	h := &ScannerHeap{}
	heap.Init(h)

	var openFiles []*os.File
	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	for _, path := range inputFiles {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		openFiles = append(openFiles, f)
		s := NewKVScanner(f)
		if s.Next() {
			heap.Push(h, s)
		}
	}

	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer out.Close()
	writer := bufio.NewWriter(out)
	defer writer.Flush()

	for h.Len() > 0 {
		s := heap.Pop(h).(*KVScanner)
		writer.Write(s.Current.Key)
		writer.WriteByte('\t')
		writer.Write(s.Current.Value)
		writer.WriteByte('\n')
		if s.Next() {
			heap.Push(h, s)
		}
	}

	return nil
}
