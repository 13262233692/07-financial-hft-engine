package model

import (
	"errors"
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

type OrderStatus uint32

const (
	OrderStatusNew       OrderStatus = 0
	OrderStatusPartially OrderStatus = 1
	OrderStatusFilled    OrderStatus = 2
	OrderStatusCancelled OrderStatus = 3
	OrderStatusRejected  OrderStatus = 4
)

var (
	ErrOrderAlreadyFilled    = errors.New("order already filled")
	ErrOrderAlreadyCancelled = errors.New("order already cancelled")
	ErrOrderAlreadyRejected  = errors.New("order already rejected")
	ErrFillExceedsRemaining  = errors.New("fill quantity exceeds remaining quantity")
)

type Order struct {
	ID        uint64
	ClOrdID   string
	Symbol    string
	Side      Side
	Type      OrderType
	Price     int64
	Quantity  int64
	FilledQty atomic.Int64
	Status    atomic.Uint32
	Timestamp int64
	SenderID  string
}

func NewOrder(id uint64, clOrdID, symbol string, side Side, otype OrderType, price, qty int64, senderID string) *Order {
	o := &Order{
		ID:        id,
		ClOrdID:   clOrdID,
		Symbol:    symbol,
		Side:      side,
		Type:      otype,
		Price:     price,
		Quantity:  qty,
		Timestamp: time.Now().UnixNano(),
		SenderID:  senderID,
	}
	o.FilledQty.Store(0)
	o.Status.Store(uint32(OrderStatusNew))
	return o
}

func (o *Order) GetStatus() OrderStatus {
	return OrderStatus(o.Status.Load())
}

func (o *Order) GetFilledQty() int64 {
	return o.FilledQty.Load()
}

func (o *Order) RemainingQty() int64 {
	return o.Quantity - o.FilledQty.Load()
}

func (o *Order) IsFilled() bool {
	return o.FilledQty.Load() >= o.Quantity
}

func (o *Order) IsActive() bool {
	s := OrderStatus(o.Status.Load())
	return s == OrderStatusNew || s == OrderStatusPartially
}

func (o *Order) TryCancel() error {
	for {
		cur := o.Status.Load()
		curStatus := OrderStatus(cur)

		if curStatus == OrderStatusFilled {
			return ErrOrderAlreadyFilled
		}
		if curStatus == OrderStatusCancelled {
			return ErrOrderAlreadyCancelled
		}
		if curStatus == OrderStatusRejected {
			return ErrOrderAlreadyRejected
		}

		if o.Status.CompareAndSwap(cur, uint32(OrderStatusCancelled)) {
			return nil
		}
	}
}

func (o *Order) TryFill(fillQty int64) error {
	if fillQty <= 0 {
		return nil
	}

	for {
		curStatus := o.GetStatus()
		if curStatus == OrderStatusCancelled || curStatus == OrderStatusRejected {
			return ErrOrderAlreadyCancelled
		}

		curFilled := o.FilledQty.Load()
		remaining := o.Quantity - curFilled
		if fillQty > remaining {
			return ErrFillExceedsRemaining
		}

		if o.FilledQty.CompareAndSwap(curFilled, curFilled+fillQty) {
			newFilled := curFilled + fillQty
			var newStatus OrderStatus
			if newFilled >= o.Quantity {
				newStatus = OrderStatusFilled
			} else {
				newStatus = OrderStatusPartially
			}

			for {
				statusCur := o.Status.Load()
				if OrderStatus(statusCur) == OrderStatusCancelled || OrderStatus(statusCur) == OrderStatusRejected {
					o.FilledQty.Add(-fillQty)
					return ErrOrderAlreadyCancelled
				}
				if o.Status.CompareAndSwap(statusCur, uint32(newStatus)) {
					break
				}
			}
			return nil
		}
	}
}

func (o *Order) CancelQuantity() int64 {
	s := o.GetStatus()
	if s != OrderStatusCancelled {
		return 0
	}
	filled := o.GetFilledQty()
	return o.Quantity - filled
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
