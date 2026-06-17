package algo

import (
	"sync/atomic"

	"github.com/hft-engine/model"
)

type AlgoType int

const (
	AlgoTWAP AlgoType = 1
)

type ParentOrderStatus int32

const (
	ParentStatusNew       ParentOrderStatus = 0
	ParentStatusRunning   ParentOrderStatus = 1
	ParentStatusPaused    ParentOrderStatus = 2
	ParentStatusCompleted ParentOrderStatus = 3
	ParentStatusCancelled ParentOrderStatus = 4
)

type ExecutionStatus int32

const (
	ChildPending  ExecutionStatus = 0
	ChildSent     ExecutionStatus = 1
	ChildFilled   ExecutionStatus = 2
	ChildCancelled ExecutionStatus = 3
)

type ParentOrder struct {
	ID            uint64
	ClientID      string
	Symbol        string
	Side          model.Side
	TotalQty      int64
	StartTime     int64
	EndTime       int64
	AlgoType      AlgoType
	MaxPrice      int64
	MinIntervalMs int64
	SliceCount    int
	PerturbPct    float64
	Status        atomic.Int32
	FilledQty     atomic.Int64
	CreatedBy     string
	CreatedAt     int64
}

type ChildOrder struct {
	ID           uint64
	ParentID     uint64
	Symbol       string
	Side         model.Side
	Qty          int64
	Price        int64
	ScheduledAt  int64
	SentAt       int64
	Status       atomic.Int32
	FilledQty    atomic.Int64
	ExternalID   atomic.Uint64
}

type ExecutionReport struct {
	ParentID     uint64
	ChildID      uint64
	OrderID      uint64
	Symbol       string
	Qty          int64
	FilledQty    int64
	Price        int64
	AvgPrice     int64
	Status       model.OrderStatus
	Timestamp    int64
}

type ExecutionClient interface {
	SendOrder(order *model.Order) (*ExecutionReport, error)
	CancelOrder(symbol string, orderID uint64) error
}

type Strategy interface {
	Name() string
	GenerateSlices(parent *ParentOrder) ([]*ChildOrder, error)
	Validate(parent *ParentOrder) error
}
