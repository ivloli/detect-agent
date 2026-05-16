package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// IsPortOccupiedWindows windows下检查端口是否被占用
func IsPortOccupiedWindows(port int) (bool, error) {
	// 查端口
	cmd := exec.Command("cmd", "/C",
		fmt.Sprintf("netstat -ano | findstr :%d", port))

	output, err := cmd.Output()
	if err != nil {
		return false, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(output))

	pids := map[int]bool{}

	for scanner.Scan() {
		line := scanner.Text()

		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		pidStr := fields[len(fields)-1]

		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			continue
		}

		pids[pid] = true
	}
	return len(pids) > 0, nil
}
