package engine

import (
	"container/heap"
	"sync"

	"github.com/hft-engine/model"
)

type priceLevel struct {
	price  int64
	orders []*model.Order
}

type buyHeap []*priceLevel

func (h buyHeap) Len() int            { return len(h) }
func (h buyHeap) Less(i, j int) bool  { return h[i].price > h[j].price }
func (h buyHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *buyHeap) Push(x interface{}) { *h = append(*h, x.(*priceLevel)) }
func (h *buyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type sellHeap []*priceLevel

func (h sellHeap) Len() int            { return len(h) }
func (h sellHeap) Less(i, j int) bool  { return h[i].price < h[j].price }
func (h sellHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *sellHeap) Push(x interface{}) { *h = append(*h, x.(*priceLevel)) }
func (h *sellHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type OrderBook struct {
	symbol     string
	mu         sync.Mutex
	bids       buyHeap
	asks       sellHeap
	bidMap     map[int64]*priceLevel
	askMap     map[int64]*priceLevel
	orderMap   map[uint64]*model.Order
	tradeOutCh chan<- *model.Trade
}

func NewOrderBook(symbol string, tradeOutCh chan<- *model.Trade) *OrderBook {
	ob := &OrderBook{
		symbol:   symbol,
		bidMap:   make(map[int64]*priceLevel),
		askMap:   make(map[int64]*priceLevel),
		orderMap: make(map[uint64]*model.Order),
		tradeOutCh: tradeOutCh,
	}
	heap.Init(&ob.bids)
	heap.Init(&ob.asks)
	return ob
}

func (ob *OrderBook) Symbol() string {
	return ob.symbol
}

func (ob *OrderBook) Submit(order *model.Order) []*model.Trade {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var trades []*model.Trade

	if order.Type == model.OrderTypeMarket {
		trades = ob.matchMarketOrder(order)
	} else {
		trades = ob.matchLimitOrder(order)
	}

	for _, t := range trades {
		if ob.tradeOutCh != nil {
			ob.tradeOutCh <- t
		}
	}

	return trades
}

func (ob *OrderBook) Cancel(orderID uint64) bool {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	order, exists := ob.orderMap[orderID]
	if !exists {
		return false
	}

	order.Status = model.OrderStatusCancelled

	if order.Side == model.SideBuy {
		if level, ok := ob.bidMap[order.Price]; ok {
			ob.removeOrderFromLevel(level, orderID)
			ob.cleanupLevel(level, ob.bidMap, &ob.bids, true)
		}
	} else {
		if level, ok := ob.askMap[order.Price]; ok {
			ob.removeOrderFromLevel(level, orderID)
			ob.cleanupLevel(level, ob.askMap, &ob.asks, false)
		}
	}

	delete(ob.orderMap, orderID)
	return true
}

func (ob *OrderBook) matchMarketOrder(order *model.Order) []*model.Trade {
	var trades []*model.Trade
	remaining := order.Quantity

	if order.Side == model.SideBuy {
		for ob.asks.Len() > 0 && remaining > 0 {
			best := ob.asks[0]
			trades = ob.matchAgainstLevel(&remaining, order, best, &ob.asks, ob.askMap, false)
		}
	} else {
		for ob.bids.Len() > 0 && remaining > 0 {
			best := ob.bids[0]
			trades = ob.matchAgainstLevel(&remaining, order, best, &ob.bids, ob.bidMap, true)
		}
	}

	order.FilledQty = order.Quantity - remaining
	if order.FilledQty >= order.Quantity {
		order.Status = model.OrderStatusFilled
	} else if order.FilledQty > 0 {
		order.Status = model.OrderStatusPartially
	}

	return trades
}

func (ob *OrderBook) matchLimitOrder(order *model.Order) []*model.Trade {
	var trades []*model.Trade
	remaining := order.Quantity

	if order.Side == model.SideBuy {
		for ob.asks.Len() > 0 && remaining > 0 {
			best := ob.asks[0]
			if order.Price < best.price {
				break
			}
			trades = ob.matchAgainstLevel(&remaining, order, best, &ob.asks, ob.askMap, false)
		}
	} else {
		for ob.bids.Len() > 0 && remaining > 0 {
			best := ob.bids[0]
			if order.Price > best.price {
				break
			}
			trades = ob.matchAgainstLevel(&remaining, order, best, &ob.bids, ob.bidMap, true)
		}
	}

	order.FilledQty = order.Quantity - remaining
	if order.IsFilled() {
		order.Status = model.OrderStatusFilled
	} else if order.FilledQty > 0 {
		order.Status = model.OrderStatusPartially
		ob.addOrderToBook(order)
	} else {
		order.Status = model.OrderStatusNew
		ob.addOrderToBook(order)
	}

	return trades
}

func (ob *OrderBook) matchAgainstLevel(remaining *int64, taker *model.Order, level *priceLevel, priceHeap heap.Interface, priceMap map[int64]*priceLevel, isBid bool) []*model.Trade {
	var trades []*model.Trade

	for len(level.orders) > 0 && *remaining > 0 {
		maker := level.orders[0]
		matchQty := min(*remaining, maker.RemainingQty())
		matchPrice := maker.Price

		trade := model.NewTrade(taker, maker, matchPrice, matchQty)
		trades = append(trades, trade)

		taker.FilledQty += matchQty
		maker.FilledQty += matchQty
		*remaining -= matchQty

		if maker.IsFilled() {
			maker.Status = model.OrderStatusFilled
			level.orders = level.orders[1:]
			delete(ob.orderMap, maker.ID)
		} else {
			maker.Status = model.OrderStatusPartially
		}
	}

	ob.cleanupLevel(level, priceMap, priceHeap, isBid)

	return trades
}

func (ob *OrderBook) addOrderToBook(order *model.Order) {
	ob.orderMap[order.ID] = order

	if order.Side == model.SideBuy {
		level, exists := ob.bidMap[order.Price]
		if !exists {
			level = &priceLevel{price: order.Price}
			ob.bidMap[order.Price] = level
			heap.Push(&ob.bids, level)
		}
		level.orders = append(level.orders, order)
	} else {
		level, exists := ob.askMap[order.Price]
		if !exists {
			level = &priceLevel{price: order.Price}
			ob.askMap[order.Price] = level
			heap.Push(&ob.asks, level)
		}
		level.orders = append(level.orders, order)
	}
}

func (ob *OrderBook) removeOrderFromLevel(level *priceLevel, orderID uint64) {
	for i, o := range level.orders {
		if o.ID == orderID {
			level.orders = append(level.orders[:i], level.orders[i+1:]...)
			return
		}
	}
}

func (ob *OrderBook) cleanupLevel(level *priceLevel, priceMap map[int64]*priceLevel, priceHeap heap.Interface, isBid bool) {
	if len(level.orders) == 0 {
		delete(priceMap, level.price)
		var slice []*priceLevel
		if isBid {
			slice = ob.bids
		} else {
			slice = ob.asks
		}
		for i, l := range slice {
			if l.price == level.price {
				heap.Remove(priceHeap, i)
				return
			}
		}
	}
}

func (ob *OrderBook) BestBid() (int64, bool) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	if ob.bids.Len() == 0 {
		return 0, false
	}
	return ob.bids[0].price, true
}

func (ob *OrderBook) BestAsk() (int64, bool) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	if ob.asks.Len() == 0 {
		return 0, false
	}
	return ob.asks[0].price, true
}

func (ob *OrderBook) Depth() (bids []struct{ Price, Qty int64 }, asks []struct{ Price, Qty int64 }) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	for _, level := range ob.bids {
		var totalQty int64
		for _, o := range level.orders {
			totalQty += o.RemainingQty()
		}
		if totalQty > 0 {
			bids = append(bids, struct{ Price, Qty int64 }{level.price, totalQty})
		}
	}
	for _, level := range ob.asks {
		var totalQty int64
		for _, o := range level.orders {
			totalQty += o.RemainingQty()
		}
		if totalQty > 0 {
			asks = append(asks, struct{ Price, Qty int64 }{level.price, totalQty})
		}
	}
	return
}
