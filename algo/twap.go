package algo

import (
	"fmt"
	"math/rand"
	"time"
)

type TWAPStrategy struct {
	rng *rand.Rand
}

func NewTWAPStrategy() *TWAPStrategy {
	return &TWAPStrategy{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *TWAPStrategy) Name() string {
	return "TWAP"
}

func (s *TWAPStrategy) Validate(parent *ParentOrder) error {
	if parent == nil {
		return fmt.Errorf("parent order is nil")
	}
	if parent.TotalQty <= 0 {
		return fmt.Errorf("total quantity must be positive")
	}
	if parent.EndTime <= parent.StartTime {
		return fmt.Errorf("end time must be after start time")
	}
	if parent.SliceCount <= 0 {
		return fmt.Errorf("slice count must be positive")
	}
	if parent.PerturbPct < 0 || parent.PerturbPct > 1.0 {
		return fmt.Errorf("perturbation percentage must be between 0 and 1.0")
	}
	if parent.MaxPrice <= 0 && parent.Side == 1 {
		return fmt.Errorf("max price must be set for buy orders")
	}
	return nil
}

func (s *TWAPStrategy) GenerateSlices(parent *ParentOrder) ([]*ChildOrder, error) {
	if err := s.Validate(parent); err != nil {
		return nil, err
	}

	start := time.Unix(0, parent.StartTime)
	end := time.Unix(0, parent.EndTime)
	totalDuration := end.Sub(start)
	numSlices := parent.SliceCount

	if numSlices < 100 {
		numSlices = 100
	}
	if numSlices > 10000 {
		numSlices = 10000
	}

	interval := totalDuration / time.Duration(numSlices)
	minInterval := time.Duration(parent.MinIntervalMs) * time.Millisecond
	if interval < minInterval && minInterval > 0 {
		interval = minInterval
		numSlices = int(totalDuration / interval)
		if numSlices < 1 {
			numSlices = 1
		}
	}

	baseQty := parent.TotalQty / int64(numSlices)
	remainder := parent.TotalQty - baseQty*int64(numSlices)

	if baseQty <= 0 {
		return nil, fmt.Errorf("slice quantity too small: baseQty=%d, increase SliceCount", baseQty)
	}

	children := make([]*ChildOrder, 0, numSlices)
	var childSeq uint64
	allocatedQty := int64(0)
	perturbPct := parent.PerturbPct
	if perturbPct <= 0 {
		perturbPct = 0.10
	}

	for i := 0; i < numSlices; i++ {
		qty := baseQty
		if int64(i) < remainder {
			qty++
		}

		perturbFactor := 1.0 + (s.rng.Float64()*2-1.0)*perturbPct
		if perturbFactor < 0.1 {
			perturbFactor = 0.1
		}
		perturbedQty := int64(float64(qty) * perturbFactor)

		if perturbedQty <= 0 {
			perturbedQty = 1
		}

		remaining := parent.TotalQty - allocatedQty
		if remaining <= 0 {
			break
		}
		if perturbedQty > remaining {
			perturbedQty = remaining
		}
		if i == numSlices-1 {
			perturbedQty = remaining
		}

		timeJitter := time.Duration(0)
		if interval > time.Millisecond {
			jitterRange := interval / 10
			if jitterRange > 0 {
				timeJitter = time.Duration(s.rng.Int63n(int64(jitterRange)))
				if s.rng.Intn(2) == 0 {
					timeJitter = -timeJitter
				}
			}
		}

		scheduledTime := start.Add(interval*time.Duration(i) + timeJitter)
		if scheduledTime.Before(start) {
			scheduledTime = start
		}
		if scheduledTime.After(end) {
			scheduledTime = end
		}

		price := parent.MaxPrice
		if parent.Side == 2 && parent.MaxPrice > 0 {
			price = parent.MaxPrice
		}

		child := &ChildOrder{
			ID:          uint64(parent.ID)<<32 | (childSeq + 1),
			ParentID:    parent.ID,
			Symbol:      parent.Symbol,
			Side:        parent.Side,
			Qty:         perturbedQty,
			Price:       price,
			ScheduledAt: scheduledTime.UnixNano(),
		}
		child.Status.Store(int32(ChildPending))

		children = append(children, child)
		allocatedQty += perturbedQty
		childSeq++
	}

	if allocatedQty != parent.TotalQty {
		diff := parent.TotalQty - allocatedQty
		if diff > 0 && len(children) > 0 {
			last := children[len(children)-1]
			last.Qty += diff
		} else if diff < 0 && len(children) > 0 {
			last := children[len(children)-1]
			last.Qty += diff
			if last.Qty < 1 {
				last.Qty = 1
			}
		}
	}

	return children, nil
}

func EstimateSliceCount(duration time.Duration, preferredInterval time.Duration) int {
	if preferredInterval <= 0 {
		preferredInterval = 30 * time.Second
	}
	count := int(duration / preferredInterval)
	if count < 100 {
		count = 100
	}
	if count > 10000 {
		count = 10000
	}
	return count
}
