package util

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type cpuStats struct {
	Idle  uint64
	Total uint64
}

// getCpuStats retrieves current snapshot of CPU ticks
func getCpuStats() (cpuStats, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return cpuStats{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		if len(fields) < 5 || fields[0] != "cpu" {
			return cpuStats{}, fmt.Errorf("can't parse /proc/stat\n")
		}

		var total uint64
		var idle uint64

		for i := 1; i < len(fields); i++ {
			val, err := strconv.ParseUint(fields[i], 10, 64)
			if err != nil {
				continue
			}
			total += val
			if i == 4 || i == 5 {
				idle += val
			}
		}
		return cpuStats{Idle: idle, Total: total}, nil
	}

	return cpuStats{}, fmt.Errorf("failed to read /proc/stat\n")
}

func GetCpuUsage(duration time.Duration) (uint8, error) {
	stat1, err := getCpuStats()
	if err != nil {
		return 0, err
	}

	time.Sleep(duration)

	stat2, err := getCpuStats()
	if err != nil {
		return 0, err
	}

	idleDelta := stat2.Idle - stat1.Idle
	totalDelta := stat2.Total - stat1.Total

	if totalDelta == 0 {
		return 0, nil
	}

	cpuPercent := float64(totalDelta-idleDelta) / float64(totalDelta) * 100
	return uint8(cpuPercent), nil
}

func GetMemoryUsage() (uint8, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var memTotal, memAvailable float64
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		if fields[0] == "MemTotal:" {
			memTotal, _ = strconv.ParseFloat(fields[1], 64)
		} else if fields[0] == "MemAvailable:" {
			memAvailable, _ = strconv.ParseFloat(fields[1], 64)
		}

		if memTotal > 0 && memAvailable > 0 {
			break
		}
	}

	if memTotal == 0 {
		return 0, fmt.Errorf("無法取得 MemTotal")
	}

	memPercent := ((memTotal - memAvailable) / memTotal) * 100
	return uint8(memPercent), nil
}
