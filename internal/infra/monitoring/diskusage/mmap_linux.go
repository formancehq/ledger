//go:build linux

package diskusage

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// mmapRSSBytes parses /proc/self/smaps and returns the total resident set size
// (in bytes) of all memory mappings backed by filePath. This captures the
// physical RAM consumed by Pebble's mmap, which is invisible to Go's runtime
// memory stats.
func mmapRSSBytes(filePath string) (int64, error) {
	f, err := os.Open("/proc/self/smaps")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var (
		total   int64
		inMatch bool
	)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// Header lines look like:
		//   7f1234000000-7f1235000000 r--s 00000000 08:01 12345  /path/to/file
		// Rss lines look like:
		//   Rss:               1024 kB
		if len(line) > 0 && line[0] >= '0' && line[0] <= '9' || (len(line) > 0 && line[0] >= 'a' && line[0] <= 'f') {
			inMatch = strings.HasSuffix(line, filePath)
			continue
		}

		if inMatch && strings.HasPrefix(line, "Rss:") {
			inMatch = false // only count once per mapping header
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					total += kb * 1024 // kB → bytes
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return total, nil
}
