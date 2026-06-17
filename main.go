package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hft-engine/engine"
	"github.com/hft-engine/gateway/fix"
	"github.com/hft-engine/model"
	"github.com/hft-engine/transport"
)

func main() {
	fixAddr := flag.String("fix-addr", ":9880", "FIX 4.4 TCP gateway listen address")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis server address")
	redisStream := flag.String("redis-stream", "hft:trades", "Redis Streams stream name")
	ringSize := flag.Uint("ring-size", 65536, "Ring buffer size (power of 2 recommended)")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[MAIN] Starting HFT Engine...")

	tradeCh := make(chan *model.Trade, 65536)
	orderCh := make(chan *model.Order, 65536)

	me := engine.NewMatchingEngine(tradeCh, orderCh, uint64(*ringSize))

	publisher := transport.NewTradePublisher(*redisAddr, *redisStream, tradeCh)

	gw := fix.NewGateway(*fixAddr, "HFTENGINE", me)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	me.Start()

	if err := publisher.HealthCheck(ctx); err != nil {
		log.Printf("[WARN] Redis health check failed: %v (will retry on publish)", err)
	} else {
		log.Println("[MAIN] Redis connection OK")
	}
	publisher.Start(ctx)

	if err := gw.Start(ctx); err != nil {
		log.Fatalf("[FATAL] Failed to start FIX gateway: %v", err)
	}

	go func() {
		for result := range me.ResultCh() {
			_ = result
		}
	}()

	log.Println("[MAIN] HFT Engine started successfully")
	log.Printf("[MAIN] FIX Gateway: %s", *fixAddr)
	log.Printf("[MAIN] Redis Stream: %s @ %s", *redisStream, *redisAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("[MAIN] Shutdown signal received, cleaning up...")

	cancel()
	gw.Stop()
	me.Stop()

	close(tradeCh)
	close(orderCh)

	log.Println("[MAIN] HFT Engine stopped")
}
