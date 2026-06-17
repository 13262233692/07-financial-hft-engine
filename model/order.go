package model

import (
	"sync/atomic"
	"time"
)

type Side uint8

const (
	SideBuy  Side = 1
	SideSell Side = 2
)

type OrderType uint8

const (
	OrderTypeLimit  OrderType = 1
	OrderTypeMarket OrderType = 2
)

type OrderStatus uint8

const (
	OrderStatusNew        OrderStatus = 0
	OrderStatusPartially  OrderStatus = 1
	OrderStatusFilled     OrderStatus = 2
	OrderStatusCancelled  OrderStatus = 3
	OrderStatusRejected   OrderStatus = 4
)

type Order struct {
	ID        uint64
	ClOrdID   string
	Symbol    string
	Side      Side
	Type      OrderType
	Price     int64
	Quantity  int64
	FilledQty int64
	Status    OrderStatus
	Timestamp int64
	SenderID  string
}

func NewOrder(id uint64, clOrdID, symbol string, side Side, otype OrderType, price, qty int64, senderID string) *Order {
	return &Order{
		ID:        id,
		ClOrdID:   clOrdID,
		Symbol:    symbol,
		Side:      side,
		Type:      otype,
		Price:     price,
		Quantity:  qty,
		FilledQty: 0,
		Status:    OrderStatusNew,
		Timestamp: time.Now().UnixNano(),
		SenderID:  senderID,
	}
}

func (o *Order) RemainingQty() int64 {
	return o.Quantity - o.FilledQty
}

func (o *Order) IsFilled() bool {
	return o.FilledQty >= o.Quantity
}

type Trade struct {
	ID          uint64
	BuyOrderID  uint64
	SellOrderID uint64
	Symbol      string
	Price       int64
	Quantity    int64
	Timestamp   int64
	BuyerID     string
	SellerID    string
}

var tradeIDCounter uint64

func NewTrade(buyOrder, sellOrder *Order, price, qty int64) *Trade {
	return &Trade{
		ID:          atomic.AddUint64(&tradeIDCounter, 1),
		BuyOrderID:  buyOrder.ID,
		SellOrderID: sellOrder.ID,
		Symbol:      buyOrder.Symbol,
		Price:       price,
		Quantity:    qty,
		Timestamp:   time.Now().UnixNano(),
		BuyerID:     buyOrder.SenderID,
		SellerID:    sellOrder.SenderID,
	}
}
