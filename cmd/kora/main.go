// Command kora is a small REPL for exercising the Kora engine.
//
// Usage:
//
//	kora [-dir ./data] [-no-sync]
//
// Commands (one per line):
//
//	set <key> <value...>   store value (value may contain spaces)
//	get <key>              print value or (nil)
//	del <key>              delete key
//	scan <start> [end]     print all live keys in [start, end]; omit end for open-ended
//	keys                   print number of live keys
//	compact                merge immutable segments, reclaiming space
//	compact-sst            merge all SSTables into one (drops tombstones)
//	stats                  print live keys, segment count, SSTable count, disk usage
//	help                   list commands
//	exit | quit            close and exit
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/giova/kora-engine/internal/store"
)

func main() {
	dir := flag.String("dir", "./data", "data directory")
	noSync := flag.Bool("no-sync", false, "disable fsync on every write (faster, less durable)")
	seg := flag.Int64("seg", 0, "max segment size in bytes before rollover (0 = default 4 MiB)")
	flag.Parse()

	db, err := store.Open(*dir, store.Options{SyncOnWrite: !*noSync, MaxSegmentBytes: *seg})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("Kora REPL — dir=%s sync=%v. Type 'help' for commands.\n", *dir, !*noSync)

	sc := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			if quit := dispatch(db, line); quit {
				break
			}
		}
		fmt.Print("> ")
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "input:", err)
	}
}

// dispatch runs one command line and returns true if the REPL should exit.
func dispatch(db *store.DB, line string) bool {
	parts := strings.SplitN(line, " ", 3)
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "set":
		if len(parts) < 3 {
			fmt.Println("usage: set <key> <value>")
			return false
		}
		if err := db.Set([]byte(parts[1]), []byte(parts[2])); err != nil {
			fmt.Println("ERR", err)
			return false
		}
		fmt.Println("OK")

	case "get":
		if len(parts) < 2 {
			fmt.Println("usage: get <key>")
			return false
		}
		val, ok, err := db.Get([]byte(parts[1]))
		if err != nil {
			fmt.Println("ERR", err)
			return false
		}
		if !ok {
			fmt.Println("(nil)")
		} else {
			fmt.Printf("%q\n", val)
		}

	case "del", "delete":
		if len(parts) < 2 {
			fmt.Println("usage: del <key>")
			return false
		}
		if err := db.Delete([]byte(parts[1])); err != nil {
			fmt.Println("ERR", err)
			return false
		}
		fmt.Println("OK")

	case "keys":
		fmt.Println(db.Len())

	case "scan":
		if len(parts) < 2 {
			fmt.Println("usage: scan <start> [end]")
			return false
		}
		start := []byte(parts[1])
		var end []byte
		if len(parts) >= 3 {
			end = []byte(parts[2])
		}
		next := db.Scan(start, end)
		count := 0
		for {
			k, v, ok := next()
			if !ok {
				break
			}
			fmt.Printf("%q -> %q\n", k, v)
			count++
		}
		fmt.Printf("(%d keys)\n", count)

	case "compact":
		if err := db.Compact(); err != nil {
			fmt.Println("ERR", err)
			return false
		}
		fmt.Println("OK")

	case "compact-sst":
		if err := db.CompactSSTables(); err != nil {
			fmt.Println("ERR", err)
			return false
		}
		fmt.Printf("OK (sstables=%d)\n", db.SSTableCount())

	case "stats":
		fmt.Printf("keys=%d segments=%d sstables=%d disk=%d bytes\n",
			db.Len(), db.SegmentCount(), db.SSTableCount(), db.DiskUsage())

	case "help":
		fmt.Println("commands: set <k> <v> | get <k> | del <k> | scan <start> [end] | keys | compact | compact-sst | stats | help | exit")

	case "exit", "quit":
		return true

	default:
		fmt.Printf("unknown command %q (try 'help')\n", cmd)
	}
	return false
}
