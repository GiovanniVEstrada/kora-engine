// Command strata is a small REPL for exercising the StrataDB engine.
//
// Usage:
//
//	strata [-dir ./data] [-no-sync]
//
// Commands (one per line):
//
//	set <key> <value...>   store value (value may contain spaces)
//	get <key>              print value or (nil)
//	del <key>              delete key
//	keys                   print number of live keys
//	compact                merge immutable segments, reclaiming space
//	stats                  print live keys, segment count, disk usage
//	help                   list commands
//	exit | quit            close and exit
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/giova/strata-engine/internal/store"
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

	fmt.Printf("StrataDB REPL — dir=%s sync=%v. Type 'help' for commands.\n", *dir, !*noSync)

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

	case "compact":
		if err := db.Compact(); err != nil {
			fmt.Println("ERR", err)
			return false
		}
		fmt.Println("OK")

	case "stats":
		fmt.Printf("keys=%d segments=%d disk=%d bytes\n",
			db.Len(), db.SegmentCount(), db.DiskUsage())

	case "help":
		fmt.Println("commands: set <k> <v> | get <k> | del <k> | keys | compact | stats | help | exit")

	case "exit", "quit":
		return true

	default:
		fmt.Printf("unknown command %q (try 'help')\n", cmd)
	}
	return false
}
