// Command kora-server starts the Kora TCP server.
//
// Usage:
//
//	kora-server [-dir ./data] [-addr :6380] [-no-sync]
//
// The server speaks RESP2 (Redis Serialization Protocol v2), so redis-cli and
// any Redis client library work out of the box:
//
//	redis-cli -p 6380 SET hello world
//	redis-cli -p 6380 GET hello
//
// For quick manual testing without redis-cli, telnet also works:
//
//	telnet localhost 6380
//	PING
//	SET hello world
//	GET hello
//
// Commands: SET  GET  DEL  DBSIZE  PING
// Kora-specific: SCAN start end  COMPACT  COMPACT-SST
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	koraserver "github.com/giova/kora-engine/internal/server"
	"github.com/giova/kora-engine/internal/store"
)

func main() {
	dir := flag.String("dir", "./data", "data directory")
	addr := flag.String("addr", ":6380", "TCP listen address")
	noSync := flag.Bool("no-sync", false, "disable fsync on every write (faster, less durable)")
	seg := flag.Int64("seg", 0, "max segment size in bytes (0 = default 4 MiB)")
	flag.Parse()

	db, err := store.Open(*dir, store.Options{
		SyncOnWrite:     !*noSync,
		MaxSegmentBytes: *seg,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()

	srv, err := koraserver.New(db, *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}

	log.Printf("kora-server listening on %s  (dir=%s sync=%v)", srv.Addr(), *dir, !*noSync)
	log.Printf("connect with: redis-cli -p %s", srv.Addr().String())

	// Shut down cleanly on SIGTERM / SIGINT.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-quit
		log.Println("shutting down...")
		srv.Close()
	}()

	if err := srv.Serve(); err != nil {
		// ErrClosed is expected on shutdown — anything else is real.
		log.Println("server stopped:", err)
	}
}
