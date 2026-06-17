package engine

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/hft-engine/model"
)

type MatchingEngine struct {
	orderBooks   map[string]*OrderBook
	mu           sync.RWMutex
	orderIDSeq   uint64
	tradeOutCh   chan<- *model.Trade
	orderInCh    <-chan *model.Order
	resultCh     chan *OrderResult
	ringBuffer   *model.RingBuffer
}

type OrderResult struct {
	Order  *model.Order
	Trades []*model.Trade
	Error  error
}

type CancelEngineResult struct {
	Success      bool
	CancelledQty int64
	Error        error
}

func NewMatchingEngine(tradeOutCh chan<- *model.Trade, orderInCh <-chan *model.Order, ringSize uint64) *MatchingEngine {
	if ringSize == 0 {
		ringSize = 65536
	}
	return &MatchingEngine{
		orderBooks: make(map[string]*OrderBook),
		tradeOutCh: tradeOutCh,
		orderInCh:  orderInCh,
		resultCh:   make(chan *OrderResult, 4096),
		ringBuffer: model.NewRingBuffer(ringSize),
	}
}

func (me *MatchingEngine) ResultCh() <-chan *OrderResult {
	return me.resultCh
}

func (me *MatchingEngine) GetOrCreateOrderBook(symbol string) *OrderBook {
	me.mu.Lock()
	defer me.mu.Unlock()

	ob, exists := me.orderBooks[symbol]
	if !exists {
		ob = NewOrderBook(symbol, me.tradeOutCh)
		me.orderBooks[symbol] = ob
	}
	return ob
}

func (me *MatchingEngine) SubmitOrder(order *model.Order) *OrderResult {
	order.ID = atomic.AddUint64(&me.orderIDSeq, 1)
	me.ringBuffer.Push(order)

	ob := me.GetOrCreateOrderBook(order.Symbol)
	trades := ob.Submit(order)

	result := &OrderResult{
		Order:  order,
		Trades: trades,
	}

	select {
	case me.resultCh <- result:
	default:
		log.Printf("[WARN] result channel full, dropping result for order %d", order.ID)
	}

	return result
}

func (me *MatchingEngine) CancelOrder(symbol string, orderID uint64) *CancelEngineResult {
	me.mu.RLock()
	ob, exists := me.orderBooks[symbol]
	me.mu.RUnlock()

	if !exists {
		return &CancelEngineResult{
			Success: false, Error: model.ErrOrderAlreadyFilled,
		}
	}

	result := ob.Cancel(orderID)
	return &CancelEngineResult{
		Success:      result.Success,
		CancelledQty: result.CancelledQty,
		Error:      result.Error,
	}
}

func (me *MatchingEngine) Start() {
	go func() {
		for order := range me.orderInCh {
			me.SubmitOrder(order)
		}
		log.Println("[INFO] matching engine order inlet closed")
	}()

	go func() {
		for {
			item, ok := me.ringBuffer.Pop()
			if !ok {
				continue
			}
			_ = item
		}
	}()
}

func (me *MatchingEngine) Stop() {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.orderBooks = make(map[string]*OrderBook)
}

func (me *MatchingEngine) GetOrderBook(symbol string) (*OrderBook, bool) {
	me.mu.RLock()
	defer me.mu.RUnlock()
	ob, ok := me.orderBooks[symbol]
	return ob, ok
}
