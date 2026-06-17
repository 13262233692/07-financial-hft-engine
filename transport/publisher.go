package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/hft-engine/model"
	"github.com/redis/go-redis/v9"
)

type TradePublisher struct {
	client    *redis.Client
	stream    string
	batchSize int
	ch        <-chan *model.Trade
}

func NewTradePublisher(redisAddr, stream string, ch <-chan *model.Trade) *TradePublisher {
	if stream == "" {
		stream = "hft:trades"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		PoolSize: 10,
	})
	return &TradePublisher{
		client:    rdb,
		stream:    stream,
		batchSize: 100,
		ch:        ch,
	}
}

func (tp *TradePublisher) Start(ctx context.Context) {
	go tp.publishLoop(ctx)
}

func (tp *TradePublisher) publishLoop(ctx context.Context) {
	batch := make([]*model.Trade, 0, tp.batchSize)

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				tp.flushBatch(ctx, batch)
			}
			tp.client.Close()
			log.Println("[INFO] trade publisher stopped")
			return
		case trade, ok := <-tp.ch:
			if !ok {
				if len(batch) > 0 {
					tp.flushBatch(ctx, batch)
				}
				tp.client.Close()
				log.Println("[INFO] trade channel closed, publisher stopped")
				return
			}
			batch = append(batch, trade)
			if len(batch) >= tp.batchSize {
				tp.flushBatch(ctx, batch)
				batch = batch[:0]
			}
		}
	}
}

func (tp *TradePublisher) flushBatch(ctx context.Context, trades []*model.Trade) {
	pipe := tp.client.Pipeline()
	for _, t := range trades {
		values := tp.tradeToValues(t)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: tp.stream,
			Values: values,
			MaxLen: 100000,
			Approx: true,
		})
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("[ERROR] failed to flush trades to redis: %v", err)
		tp.retryFlush(ctx, trades)
	}
}

func (tp *TradePublisher) retryFlush(ctx context.Context, trades []*model.Trade) {
	for i := 0; i < 3; i++ {
		pipe := tp.client.Pipeline()
		for _, t := range trades {
			values := tp.tradeToValues(t)
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: tp.stream,
				Values: values,
				MaxLen: 100000,
				Approx: true,
			})
		}
		if _, execErr := pipe.Exec(ctx); execErr == nil {
			return
		} else {
			log.Printf("[WARN] retry %d failed for flush: %v", i+1, execErr)
		}
	}
	log.Printf("[ERROR] all retries exhausted for %d trades", len(trades))
}

func (tp *TradePublisher) tradeToValues(t *model.Trade) map[string]interface{} {
	return map[string]interface{}{
		"trade_id":   strconv.FormatUint(t.ID, 10),
		"symbol":     t.Symbol,
		"price":      strconv.FormatInt(t.Price, 10),
		"quantity":   strconv.FormatInt(t.Quantity, 10),
		"buy_order":  strconv.FormatUint(t.BuyOrderID, 10),
		"sell_order": strconv.FormatUint(t.SellOrderID, 10),
		"buyer":      t.BuyerID,
		"seller":     t.SellerID,
		"timestamp":  strconv.FormatInt(t.Timestamp, 10),
	}
}

func (tp *TradePublisher) Publish(ctx context.Context, trade *model.Trade) error {
	values := tp.tradeToValues(trade)
	err := tp.client.XAdd(ctx, &redis.XAddArgs{
		Stream: tp.stream,
		Values: values,
		MaxLen: 100000,
		Approx: true,
	}).Err()
	if err != nil {
		return fmt.Errorf("redis XADD failed: %w", err)
	}
	return nil
}

func (tp *TradePublisher) HealthCheck(ctx context.Context) error {
	return tp.client.Ping(ctx).Err()
}

func TradeFromJSON(data []byte) (*model.Trade, error) {
	var t model.Trade
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("invalid trade JSON: %w", err)
	}
	return &t, nil
}
