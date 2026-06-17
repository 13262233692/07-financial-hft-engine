package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hft-engine/model"
)

func TestOrderTryCancelRace(t *testing.T) {
	const iterations = 100

	for i := 0; i < iterations; i++ {
		order := model.NewOrder(1, "CL1", "BTC", model.SideBuy, model.OrderTypeLimit, 100, 10, "BROKER1")

		var wg sync.WaitGroup
		var cancelSuccess, fillSuccess int32

		wg.Add(2)

		go func() {
			defer wg.Done()
			err := order.TryCancel()
			if err == nil {
				atomic.AddInt32(&cancelSuccess, 1)
			}
		}()

		go func() {
			defer wg.Done()
			err := order.TryFill(10)
			if err == nil {
				atomic.AddInt32(&fillSuccess, 1)
			}
		}()

		wg.Wait()

		if cancelSuccess > 0 && fillSuccess > 0 {
			t.Fatalf("FATAL RACE: Both cancel and fill succeeded! cancel=%d fill=%d status=%v filled=%v",
				cancelSuccess, fillSuccess, order.GetStatus(), order.GetFilledQty())
		}

		if cancelSuccess == 0 && fillSuccess == 0 {
			continue
		}

		status := order.GetStatus()
		filled := order.GetFilledQty()

		if cancelSuccess > 0 {
			if status != model.OrderStatusCancelled {
				t.Errorf("Cancel succeeded but status is %v, expected Cancelled", status)
			}
			if filled != 0 {
				t.Errorf("Cancel succeeded but filled qty is %d, expected 0", filled)
			}
		}

		if fillSuccess > 0 {
			if status != model.OrderStatusFilled {
				t.Errorf("Fill succeeded but status is %v, expected Filled", status)
			}
			if filled != 10 {
				t.Errorf("Fill succeeded but filled qty is %d, expected 10", filled)
			}
		}
	}

	t.Logf("CAS race test passed: %d iterations without double-success", iterations)
}

func TestOrderTryFillRace(t *testing.T) {
	const iterations = 100

	for i := 0; i < iterations; i++ {
		order := model.NewOrder(1, "CL1", "BTC", model.SideBuy, model.OrderTypeLimit, 100, 10, "BROKER1")

		var wg sync.WaitGroup
		var fill1Success, fill2Success int32

		wg.Add(2)

		go func() {
			defer wg.Done()
			err := order.TryFill(7)
			if err == nil {
				atomic.AddInt32(&fill1Success, 1)
			}
		}()

		go func() {
			defer wg.Done()
			err := order.TryFill(7)
			if err == nil {
				atomic.AddInt32(&fill2Success, 1)
			}
		}()

		wg.Wait()

		totalSuccess := fill1Success + fill2Success
		totalFilled := order.GetFilledQty()

		if totalSuccess == 2 {
			if totalFilled != 10 {
				t.Errorf("Both fills succeeded: totalFilled=%d expected 10", totalFilled)
			}

			status := order.GetStatus()
			if status != model.OrderStatusFilled {
				t.Errorf("Both fills succeeded but status=%v expected Filled", status)
			}
		} else if totalSuccess == 1 {
			if totalFilled != 7 {
				t.Errorf("One fill succeeded but totalFilled=%d expected 7", totalFilled)
			}

			status := order.GetStatus()
			if status != model.OrderStatusPartially {
				t.Errorf("One fill succeeded but status=%v expected Partially", status)
			}
		}
	}

	t.Logf("Fill race test passed: %d iterations", iterations)
}

func TestOrderStateTransitions(t *testing.T) {
	order := model.NewOrder(1, "CL1", "BTC", model.SideBuy, model.OrderTypeLimit, 100, 100, "BROKER1")

	if err := order.TryFill(50); err != nil {
		t.Fatalf("First fill should succeed: %v", err)
	}
	if order.GetStatus() != model.OrderStatusPartially {
		t.Errorf("Status should be Partially, got %v", order.GetStatus())
	}
	if order.GetFilledQty() != 50 {
		t.Errorf("Filled should be 50, got %d", order.GetFilledQty())
	}

	if err := order.TryFill(50); err != nil {
		t.Fatalf("Second fill should succeed: %v", err)
	}
	if order.GetStatus() != model.OrderStatusFilled {
		t.Errorf("Status should be Filled, got %v", order.GetStatus())
	}
	if order.GetFilledQty() != 100 {
		t.Errorf("Filled should be 100, got %d", order.GetFilledQty())
	}

	err := order.TryCancel()
	if err != model.ErrOrderAlreadyFilled {
		t.Errorf("Cancel of filled order should return ErrOrderAlreadyFilled, got %v", err)
	}

	order2 := model.NewOrder(2, "CL2", "BTC", model.SideBuy, model.OrderTypeLimit, 100, 100, "BROKER1")
	if err := order2.TryCancel(); err != nil {
		t.Fatalf("Cancel of new order should succeed: %v", err)
	}
	if order2.GetStatus() != model.OrderStatusCancelled {
		t.Errorf("Status should be Cancelled, got %v", order2.GetStatus())
	}

	err = order2.TryFill(50)
	if err != model.ErrOrderAlreadyCancelled {
		t.Errorf("Fill of cancelled order should return ErrOrderAlreadyCancelled, got %v", err)
	}

	order3 := model.NewOrder(3, "CL3", "BTC", model.SideBuy, model.OrderTypeLimit, 100, 100, "BROKER1")
	if err := order3.TryFill(30); err != nil {
		t.Fatalf("Partial fill should succeed: %v", err)
	}
	if err := order3.TryCancel(); err != nil {
		t.Fatalf("Cancel of partially filled order should succeed: %v", err)
	}
	if order3.GetStatus() != model.OrderStatusCancelled {
		t.Errorf("Status should be Cancelled, got %v", order3.GetStatus())
	}
	if order3.GetFilledQty() != 30 {
		t.Errorf("Filled qty should remain 30, got %d", order3.GetFilledQty())
	}
	if order3.CancelQuantity() != 70 {
		t.Errorf("CancelQuantity should be 70, got %d", order3.CancelQuantity())
	}

	t.Log("All state transition tests passed")
}

func TestOrderBookCancelVsMatch(t *testing.T) {
	const iterations = 50

	for iter := 0; iter < iterations; iter++ {
		ob := NewOrderBook("BTC", nil)

		maker := model.NewOrder(1, "MAKER1", "BTC", model.SideSell, model.OrderTypeLimit, 100, 100, "MAKER")
		ob.Submit(maker)

		var wg sync.WaitGroup
		var cancelSuccess, fillSuccess int32

		wg.Add(2)

		go func() {
			defer wg.Done()
			result := ob.Cancel(maker.ID)
			if result.Success {
				atomic.AddInt32(&cancelSuccess, 1)
			}
		}()

		go func() {
			defer wg.Done()
			taker := model.NewOrder(2, "TAKER1", "BTC", model.SideBuy, model.OrderTypeMarket, 0, 100, "TAKER")
			trades := ob.Submit(taker)
			if len(trades) > 0 {
				atomic.AddInt32(&fillSuccess, 1)
			}
		}()

		wg.Wait()

		status := maker.GetStatus()
		filled := maker.GetFilledQty()
		cancelQty := maker.CancelQuantity()

		exposure := filled + cancelQty

		if exposure > maker.Quantity {
			t.Fatalf("FATAL: Exposure %d > Qty %d! cancel=%d fill=%d status=%v filled=%d cancelQty=%d",
				exposure, maker.Quantity, cancelSuccess, fillSuccess, status, filled, cancelQty)
		}

		if cancelSuccess > 0 && fillSuccess > 0 {
			if status == model.OrderStatusCancelled && filled > 0 {
				t.Fatalf("INCONSISTENT: Status=Cancelled but filled=%d", filled)
			}
			if status == model.OrderStatusFilled && cancelQty > 0 {
				t.Fatalf("INCONSISTENT: Status=Filled but cancelQty=%d", cancelQty)
			}
		}

		if cancelSuccess > 0 {
			if maker.GetStatus() != model.OrderStatusCancelled {
				t.Errorf("Cancel succeeded but status=%v", maker.GetStatus())
			}
		}

		if fillSuccess > 0 {
			if maker.GetStatus() != model.OrderStatusFilled {
				t.Errorf("Fill succeeded but status=%v", maker.GetStatus())
			}
			if maker.GetFilledQty() != 100 {
				t.Errorf("Fill succeeded but filled=%d expected 100", maker.GetFilledQty())
			}
		}
	}

	t.Logf("OrderBook cancel/match race test passed: %d iterations", iterations)
}

func BenchmarkOrderTryFill(b *testing.B) {
	order := model.NewOrder(1, "CL1", "BTC", model.SideBuy, model.OrderTypeLimit, 100, 1000000, "BROKER1")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		order.TryFill(1)
	}
}

func BenchmarkOrderTryCancel(b *testing.B) {
	b.Run("no-contention", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			order := model.NewOrder(uint64(i), fmt.Sprintf("CL%d", i), "BTC",
				model.SideBuy, model.OrderTypeLimit, 100, 100, "BROKER1")
			order.TryCancel()
		}
	})
}
