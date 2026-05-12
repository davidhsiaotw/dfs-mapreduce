package util

import (
	"io/fs"
	"log"
	"path/filepath"
	"reflect"
	"syscall"
)

const MinAcceptedBps = 1 << 20 // 1 MiB/s

func VerifyChecksum(serverCheck []byte, clientCheck []byte) bool {
	// log.Printf("Server checksum: %x\n", serverCheck)
	// log.Printf("Client checksum: %x\n", clientCheck)
	if reflect.DeepEqual(clientCheck, serverCheck) {
		log.Println("Checksums match")
		return true
	} else {
		log.Println("Checksums DO NOT match")
		return false
	}
}

func GetDirSize(path string) uint64 {
	var size uint64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			size += uint64(info.Size())
		}
		return nil
	})

	if err != nil {
		return 0
	}
	return size
}

func GetFreeSpace(path string) uint64 {
	var stat syscall.Statfs_t

	if err := syscall.Statfs(".", &stat); err != nil {
		return 0
	}
	return stat.Bavail * uint64(stat.Bsize)
}

func DeadlineSeconds(size uint64) uint64 {
	timeoutSeconds := max(size/MinAcceptedBps, 1) * 2
	return timeoutSeconds
}
