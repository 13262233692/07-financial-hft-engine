package algo

import (
	"testing"
	"time"

	"github.com/hft-engine/model"
)

func TestTWAPSlicingBasic(t *testing.T) {
	strategy := NewTWAPStrategy()

	start := time.Unix(1700000000, 0)
	end := start.Add(2 * time.Hour)
	parent := &ParentOrder{
		ID:          1,
		Symbol:      "BTC",
		Side:        model.SideBuy,
		TotalQty:    10000,
		StartTime:   start.UnixNano(),
		EndTime:     end.UnixNano(),
		SliceCount:  200,
		PerturbPct:  0.10,
		MaxPrice:    50000,
	}

	children, err := strategy.GenerateSlices(parent)
	if err != nil {
		t.Fatalf("GenerateSlices failed: %v", err)
	}

	if len(children) == 0 {
		t.Fatal("No children generated")
	}

	totalQty := int64(0)
	for i, c := range children {
		if c.ParentID != parent.ID {
			t.Errorf("Child %d: wrong ParentID %d expected %d", i, c.ParentID, parent.ID)
		}
		if c.Symbol != parent.Symbol {
			t.Errorf("Child %d: wrong symbol", i)
		}
		if c.Side != parent.Side {
			t.Errorf("Child %d: wrong side", i)
		}
		if c.Price != parent.MaxPrice {
			t.Errorf("Child %d: wrong price %d expected %d", i, c.Price, parent.MaxPrice)
		}
		if c.Qty <= 0 {
			t.Errorf("Child %d: non-positive qty %d", i, c.Qty)
		}
		totalQty += c.Qty
	}

	if totalQty != parent.TotalQty {
		t.Errorf("Total sliced qty %d != parent qty %d (diff=%d)",
			totalQty, parent.TotalQty, totalQty-parent.TotalQty)
	}

	t.Logf("Generated %d slices, total qty=%d", len(children), totalQty)
}

func TestTWAPSlicingNoPerturb(t *testing.T) {
	strategy := NewTWAPStrategy()

	start := time.Unix(1700000000, 0)
	end := start.Add(time.Hour)
	parent := &ParentOrder{
		ID:         2,
		Symbol:     "ETH",
		Side:       model.SideSell,
		TotalQty:   1000,
		StartTime:  start.UnixNano(),
		EndTime:    end.UnixNano(),
		SliceCount: 100,
		PerturbPct: 0,
		MaxPrice:   3000,
	}

	children, err := strategy.GenerateSlices(parent)
	if err != nil {
		t.Fatalf("GenerateSlices failed: %v", err)
	}

	totalQty := int64(0)
	for _, c := range children {
		totalQty += c.Qty
	}

	if totalQty != parent.TotalQty {
		t.Errorf("Total sliced qty %d != parent qty %d", totalQty, parent.TotalQty)
	}
	t.Logf("No-perturb mode: %d slices, total=%d", len(children), totalQty)
}

func TestTWAPSlicingTimeDistribution(t *testing.T) {
	strategy := NewTWAPStrategy()

	start := time.Unix(1700000000, 0)
	end := start.Add(2 * time.Hour)
	parent := &ParentOrder{
		ID:         3,
		Symbol:     "BTC",
		Side:       model.SideBuy,
		TotalQty:   100000,
		StartTime:  start.UnixNano(),
		EndTime:    end.UnixNano(),
		SliceCount: 240,
		PerturbPct: 0.05,
		MaxPrice:   50000,
	}

	children, err := strategy.GenerateSlices(parent)
	if err != nil {
		t.Fatalf("GenerateSlices failed: %v", err)
	}

	for i, c := range children {
		if c.ScheduledAt < parent.StartTime {
			t.Errorf("Child %d: scheduled before start", i)
		}
		if c.ScheduledAt > parent.EndTime {
			t.Errorf("Child %d: scheduled after end", i)
		}
	}

	first := children[0]
	last := children[len(children)-1]
	span := time.Duration(last.ScheduledAt - first.ScheduledAt)
	expectedSpan := end.Sub(start)
	t.Logf("Time span: %v (expected ~%v), slices: %d", span, expectedSpan, len(children))
}

func TestTWAPValidation(t *testing.T) {
	strategy := NewTWAPStrategy()

	start := time.Unix(1700000000, 0)

	tests := []struct {
		name    string
		parent  *ParentOrder
		wantErr bool
	}{
		{
			name: "valid order",
			parent: &ParentOrder{
				ID: 1, Symbol: "BTC", Side: model.SideBuy, TotalQty: 100,
				StartTime: start.UnixNano(),
				EndTime:   start.Add(time.Hour).UnixNano(),
				SliceCount: 120, PerturbPct: 0.1, MaxPrice: 100,
			},
			wantErr: false,
		},
		{
			name: "negative qty",
			parent: &ParentOrder{
				ID: 2, Symbol: "BTC", Side: model.SideBuy, TotalQty: -1,
				StartTime: start.UnixNano(),
				EndTime:   start.Add(time.Hour).UnixNano(),
				SliceCount: 120, MaxPrice: 100,
			},
			wantErr: true,
		},
		{
			name: "end before start",
			parent: &ParentOrder{
				ID: 3, Symbol: "BTC", Side: model.SideBuy, TotalQty: 100,
				StartTime: start.Add(time.Hour).UnixNano(),
				EndTime:   start.UnixNano(),
				SliceCount: 120, MaxPrice: 100,
			},
			wantErr: true,
		},
		{
			name:    "nil parent",
			parent:  nil,
			wantErr: true,
		},
		{
			name: "buy without max price",
			parent: &ParentOrder{
				ID: 4, Symbol: "BTC", Side: model.SideBuy, TotalQty: 100,
				StartTime: start.UnixNano(),
				EndTime:   start.Add(time.Hour).UnixNano(),
				SliceCount: 120, MaxPrice: 0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := strategy.Validate(tt.parent)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEstimateSliceCount(t *testing.T) {
	cases := []struct {
		duration time.Duration
		interval time.Duration
	}{
		{2 * time.Hour, 30 * time.Second},
		{5 * time.Minute, 10 * time.Second},
		{10 * time.Second, time.Second},
	}

	for _, c := range cases {
		count := EstimateSliceCount(c.duration, c.interval)
		t.Logf("duration=%v interval=%v => %d slices", c.duration, c.interval, count)
	}
}
