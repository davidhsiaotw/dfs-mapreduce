//go:build delete_results

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <controller_addr> <job_id>\n", os.Args[0])
		os.Exit(1)
	}

	controllerAddr := os.Args[1]
	jobID := os.Args[2]

	listCmd := exec.Command("dfs/bin/client", controllerAddr, "list")
	output, err := listCmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing files: %v\n%s", err, string(output))
		os.Exit(1)
	}

	lines := strings.Split(string(output), "\n")
	prefix := fmt.Sprintf("res-%s-", jobID)
	deleteCount := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || line == "LIST" || line == "no files were found." {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			filename := fields[1]
			if strings.HasPrefix(filename, prefix) {
				fmt.Printf("Deleting %s...\n", filename)
				deleteCmd := exec.Command("dfs/bin/client", controllerAddr, "delete", filename)
				deleteOutput, deleteErr := deleteCmd.CombinedOutput()
				if deleteErr != nil {
					fmt.Fprintf(os.Stderr, "Error deleting %s: %v\n%s", filename, deleteErr, string(deleteOutput))
				} else {
					deleteCount++
				}
			}
		}
	}

	fmt.Printf("Deleted %d files from DFS for job %s\n", deleteCount, jobID)
}