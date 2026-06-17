package algo

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hft-engine/model"
)

type scheduledItem struct {
	child *ChildOrder
	idx   int
}

type timeHeap []*scheduledItem

func (h timeHeap) Len() int { return len(h) }
func (h timeHeap) Less(i, j int) bool {
	return h[i].child.ScheduledAt < h[j].child.ScheduledAt
}
func (h timeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}
func (h *timeHeap) Push(x interface{}) {
	item := x.(*scheduledItem)
	item.idx = len(*h)
	*h = append(*h, item)
}
func (h *timeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.idx = -1
	*h = old[:n-1]
	return item
}

type AlgoEngine struct {
	mu             sync.Mutex
	execClient     ExecutionClient
	strategies     map[AlgoType]Strategy
	parents        map[uint64]*ParentOrder
	children       map[uint64][]*ChildOrder
	childToParent  map[uint64]uint64
	schedulerHeap  timeHeap
	schedulerCh    chan struct{}
	parentIDSeq    uint64
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	reportCh       chan *ExecutionReport
	progressCb     func(uint64, int64, int64)
}

func NewAlgoEngine(execClient ExecutionClient) *AlgoEngine {
	ctx, cancel := context.WithCancel(context.Background())
	ae := &AlgoEngine{
		execClient:    execClient,
		strategies:    make(map[AlgoType]Strategy),
		parents:       make(map[uint64]*ParentOrder),
		children:      make(map[uint64][]*ChildOrder),
		childToParent: make(map[uint64]uint64),
		schedulerCh:   make(chan struct{}, 1),
		reportCh:      make(chan *ExecutionReport, 4096),
		ctx:           ctx,
		cancel:        cancel,
	}
	ae.strategies[AlgoTWAP] = NewTWAPStrategy()
	heap.Init(&ae.schedulerHeap)
	return ae
}

func (ae *AlgoEngine) OnProgress(cb func(parentID uint64, filledQty, totalQty int64)) {
	ae.progressCb = cb
}

func (ae *AlgoEngine) ReportCh() <-chan *ExecutionReport {
	return ae.reportCh
}

func (ae *AlgoEngine) Start() {
	ae.wg.Add(1)
	go ae.schedulerLoop()
	ae.wg.Add(1)
	go ae.reportLoop()
	log.Println("[ALGO] Engine started")
}

func (ae *AlgoEngine) Stop() {
	ae.cancel()
	ae.wg.Wait()
	log.Println("[ALGO] Engine stopped")
}

func (ae *AlgoEngine) SubmitParent(parent *ParentOrder) (*ParentOrder, error) {
	strategy, exists := ae.strategies[parent.AlgoType]
	if !exists {
		return nil, fmt.Errorf("unsupported algo type: %d", parent.AlgoType)
	}

	if parent.StartTime == 0 {
		parent.StartTime = time.Now().UnixNano()
	}
	if parent.EndTime == 0 {
		if parent.SliceCount > 0 {
			parent.EndTime = parent.StartTime + int64(parent.SliceCount)*int64(time.Second*30)
		} else {
			parent.EndTime = parent.StartTime + int64(time.Hour*2)
		}
	}
	if parent.SliceCount == 0 {
		dur := time.Duration(parent.EndTime - parent.StartTime)
		parent.SliceCount = EstimateSliceCount(dur, time.Second*30)
	}
	if parent.PerturbPct <= 0 {
		parent.PerturbPct = 0.10
	}

	if err := strategy.Validate(parent); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	children, err := strategy.GenerateSlices(parent)
	if err != nil {
		return nil, fmt.Errorf("slice generation failed: %w", err)
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("no child orders generated")
	}

	ae.mu.Lock()
	parent.ID = atomic.AddUint64(&ae.parentIDSeq, 1)
	parent.Status.Store(int32(ParentStatusRunning))
	parent.CreatedAt = time.Now().UnixNano()

	ae.parents[parent.ID] = parent
	ae.children[parent.ID] = children

	for _, child := range children {
		ae.childToParent[child.ID] = parent.ID
		heap.Push(&ae.schedulerHeap, &scheduledItem{child: child})
	}
	ae.mu.Unlock()

	ae.wakeScheduler()

	log.Printf("[ALGO] Parent order #%d submitted: %s %s qty=%d slices=%d window=%s",
		parent.ID, parent.Symbol, sideStr(parent.Side), parent.TotalQty, len(children),
		time.Duration(parent.EndTime-parent.StartTime))

	return parent, nil
}

func (ae *AlgoEngine) CancelParent(parentID uint64) error {
	ae.mu.Lock()
	parent, exists := ae.parents[parentID]
	if !exists {
		ae.mu.Unlock()
		return fmt.Errorf("parent order %d not found", parentID)
	}

	curStatus := ParentOrderStatus(parent.Status.Load())
	if curStatus == ParentStatusCompleted || curStatus == ParentStatusCancelled {
		ae.mu.Unlock()
		return fmt.Errorf("parent order %d already %s", parentID, statusStr(curStatus))
	}

	parent.Status.Store(int32(ParentStatusCancelled))

	children := ae.children[parentID]
	cancelledCount := 0
	for _, child := range children {
		childStatus := ExecutionStatus(child.Status.Load())
		if childStatus == ChildPending {
			child.Status.Store(int32(ChildCancelled))
			cancelledCount++
		} else if childStatus == ChildSent {
			extID := child.ExternalID.Load()
			if extID > 0 {
				_ = ae.execClient.CancelOrder(child.Symbol, extID)
			}
			child.Status.Store(int32(ChildCancelled))
			cancelledCount++
		}
	}

	ae.mu.Unlock()
	log.Printf("[ALGO] Parent order #%d cancelled (pending children=%d aborted)", parentID, cancelledCount)
	return nil
}

func (ae *AlgoEngine) GetParent(parentID uint64) (*ParentOrder, []*ChildOrder, error) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	parent, exists := ae.parents[parentID]
	if !exists {
		return nil, nil, fmt.Errorf("parent order %d not found", parentID)
	}

	children := make([]*ChildOrder, len(ae.children[parentID]))
	copy(children, ae.children[parentID])

	sort.Slice(children, func(i, j int) bool {
		return children[i].ScheduledAt < children[j].ScheduledAt
	})

	return parent, children, nil
}

func (ae *AlgoEngine) ListParents() []*ParentOrder {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	result := make([]*ParentOrder, 0, len(ae.parents))
	for _, p := range ae.parents {
		result = append(result, p)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt > result[j].CreatedAt
	})

	return result
}

func (ae *AlgoEngine) wakeScheduler() {
	select {
	case ae.schedulerCh <- struct{}{}:
	default:
	}
}

func (ae *AlgoEngine) schedulerLoop() {
	defer ae.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ae.ctx.Done():
			return
		case <-ae.schedulerCh:
			ae.processDueChildren()
		case <-ticker.C:
			ae.processDueChildren()
		}
	}
}

func (ae *AlgoEngine) processDueChildren() {
	now := time.Now().UnixNano()

	for {
		ae.mu.Lock()
		if ae.schedulerHeap.Len() == 0 {
			ae.mu.Unlock()
			return
		}

		top := ae.schedulerHeap[0].child
		if top.ScheduledAt > now {
			ae.mu.Unlock()
			return
		}

		heap.Pop(&ae.schedulerHeap)
		ae.mu.Unlock()

		status := ExecutionStatus(top.Status.Load())
		if status != ChildPending {
			continue
		}

		parentID, ok := ae.childToParent[top.ID]
		if !ok {
			continue
		}

		ae.mu.Lock()
		parent, pExists := ae.parents[parentID]
		ae.mu.Unlock()
		if !pExists {
			continue
		}

		parentStatus := ParentOrderStatus(parent.Status.Load())
		if parentStatus == ParentStatusCancelled {
			top.Status.Store(int32(ChildCancelled))
			continue
		}
		if parentStatus == ParentStatusPaused {
			top.ScheduledAt = time.Now().UnixNano() + int64(time.Second)
			ae.mu.Lock()
			heap.Push(&ae.schedulerHeap, &scheduledItem{child: top})
			ae.mu.Unlock()
			continue
		}

		ae.wg.Add(1)
		go ae.executeChild(parent, top)
	}
}

func (ae *AlgoEngine) executeChild(parent *ParentOrder, child *ChildOrder) {
	defer ae.wg.Done()

	if !child.Status.CompareAndSwap(int32(ChildPending), int32(ChildSent)) {
		return
	}

	child.SentAt = time.Now().UnixNano()

	order := model.NewOrder(0, fmt.Sprintf("A-%d-%d", child.ParentID, child.ID),
		child.Symbol, child.Side, model.OrderTypeLimit, child.Price, child.Qty,
		fmt.Sprintf("ALGO-%d", parent.ID))

	report, err := ae.execClient.SendOrder(order)
	if err != nil {
		log.Printf("[ALGO] Child %d send failed: %v", child.ID, err)
		child.Status.CompareAndSwap(int32(ChildSent), int32(ChildPending))
		child.ScheduledAt = time.Now().UnixNano() + int64(time.Second*5)
		ae.mu.Lock()
		heap.Push(&ae.schedulerHeap, &scheduledItem{child: child})
		ae.mu.Unlock()
		ae.wakeScheduler()
		return
	}

	child.ExternalID.Store(report.OrderID)
	child.FilledQty.Store(report.FilledQty)

	switch report.Status {
	case model.OrderStatusFilled:
		child.Status.Store(int32(ChildFilled))
	case model.OrderStatusPartially:
		child.Status.Store(int32(ChildSent))
	default:
		if report.FilledQty > 0 {
			child.Status.Store(int32(ChildSent))
		}
	}

	parent.FilledQty.Add(report.FilledQty)

	select {
	case ae.reportCh <- report:
	default:
	}

	if ae.progressCb != nil {
		ae.progressCb(parent.ID, parent.FilledQty.Load(), parent.TotalQty)
	}

	totalFilled := parent.FilledQty.Load()
	if totalFilled >= parent.TotalQty {
		parent.Status.Store(int32(ParentStatusCompleted))
		log.Printf("[ALGO] Parent order #%d COMPLETED: filled=%d/%d",
			parent.ID, totalFilled, parent.TotalQty)
	}
}

func (ae *AlgoEngine) reportLoop() {
	defer ae.wg.Done()
	for {
		select {
		case <-ae.ctx.Done():
			return
		case <-ae.reportCh:
		}
	}
}

func sideStr(s model.Side) string {
	if s == model.SideBuy {
		return "BUY"
	}
	return "SELL"
}

func statusStr(s ParentOrderStatus) string {
	switch s {
	case ParentStatusNew:
		return "NEW"
	case ParentStatusRunning:
		return "RUNNING"
	case ParentStatusPaused:
		return "PAUSED"
	case ParentStatusCompleted:
		return "COMPLETED"
	case ParentStatusCancelled:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}
