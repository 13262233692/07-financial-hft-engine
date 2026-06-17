package algo

import (
	"fmt"

	"github.com/hft-engine/engine"
	"github.com/hft-engine/model"
)

type DirectExecutionClient struct {
	me     *engine.MatchingEngine
	resultCh chan *engine.OrderResult
}

func NewDirectExecutionClient(me *engine.MatchingEngine) *DirectExecutionClient {
	c := &DirectExecutionClient{
		me:       me,
		resultCh: make(chan *engine.OrderResult, 8192),
	}

	go func() {
		for range c.resultCh {
		}
	}()

	return c
}

func (c *DirectExecutionClient) SendOrder(order *model.Order) (*ExecutionReport, error) {
	result := c.me.SubmitOrder(order)
	if result == nil {
		return nil, fmt.Errorf("matching engine returned nil result")
	}
	if result.Error != nil {
		return nil, result.Error
	}

	report := &ExecutionReport{
		ParentID:  0,
		ChildID:   0,
		OrderID:   result.Order.ID,
		Symbol:    result.Order.Symbol,
		Qty:       result.Order.Quantity,
		FilledQty: result.Order.GetFilledQty(),
		Price:     result.Order.Price,
		Status:    result.Order.GetStatus(),
		Timestamp: result.Order.Timestamp,
	}

	if len(result.Trades) > 0 {
		var totalValue int64
		for _, t := range result.Trades {
			totalValue += t.Price * t.Quantity
		}
		if report.FilledQty > 0 {
			report.AvgPrice = totalValue / report.FilledQty
		}
	} else if report.FilledQty > 0 {
		report.AvgPrice = result.Order.Price
	}

	return report, nil
}

func (c *DirectExecutionClient) CancelOrder(symbol string, orderID uint64) error {
	result := c.me.CancelOrder(symbol, orderID)
	if result == nil {
		return fmt.Errorf("cancel returned nil result")
	}
	if !result.Success && result.Error != nil {
		return result.Error
	}
	return nil
}
