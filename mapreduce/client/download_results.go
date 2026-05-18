//go:build download_results

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Printf("Usage: %s <controller_addr> <job_id> <download_dir>\n", os.Args[0])
		os.Exit(1)
	}

	controllerAddr := os.Args[1]
	jobID := os.Args[2]
	downloadDir := os.Args[3]

	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating download directory: %v\n", err)
		os.Exit(1)
	}

	listCmd := exec.Command("dfs/bin/client", controllerAddr, "list")
	output, err := listCmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing files: %v\n%s", err, string(output))
		os.Exit(1)
	}

	lines := strings.Split(string(output), "\n")
	prefix := fmt.Sprintf("res-%s-", jobID)
	downloadCount := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || line == "LIST" || line == "no files were found." {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			filename := fields[1]
			if strings.HasPrefix(filename, prefix) {
				fmt.Printf("Downloading %s...\n", filename)
				getCmd := exec.Command("dfs/bin/client", controllerAddr, "get", filename, downloadDir)
				getOutput, getErr := getCmd.CombinedOutput()
				if getErr != nil {
					fmt.Fprintf(os.Stderr, "Error downloading %s: %v\n%s", filename, getErr, string(getOutput))
				} else {
					downloadCount++
				}
			}
		}
	}

	fmt.Printf("Downloaded %d files to %s\n", downloadCount, downloadDir)
}