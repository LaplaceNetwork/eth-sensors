package cacher

import (
	"sort"
	"sync"
	"time"

	config "github.com/dynamicgo/go-config"
	sensors "github.com/laplacenetwork/eth-sensors"
)

type cacherImpl struct {
	sync.Mutex
	orders        []*sensors.Order
	confirmBlocks int64
	timeoutBlocks int64
}

// New .
func New(config config.Config) (sensors.OrderCacher, error) {
	return NewCacher(
		int64(config.Get("order", "confirmed").Int(1)),
		int64(config.Get("order", "timeout").Int(60)),
	), nil
}

// NewCacher .
func NewCacher(confirmBlocks int64, timeoutBlocks int64) sensors.OrderCacher {
	return newCacher(confirmBlocks, timeoutBlocks)
}

func newCacher(confirmBlocks int64, timeoutBlocks int64) sensors.OrderCacher {
	return &cacherImpl{
		confirmBlocks: confirmBlocks,
		timeoutBlocks: timeoutBlocks,
	}
}

func (cacher *cacherImpl) Cache(orders []*sensors.Order) {
	cacher.Lock()
	defer cacher.Unlock()

	cacher.orders = append(cacher.orders, orders...)

	sort.Slice(cacher.orders, func(i, j int) bool {
		return cacher.orders[i].PendingBlock < cacher.orders[j].PendingBlock
	})
}
func (cacher *cacherImpl) Mint(tx string, block int64, time time.Time) (*sensors.Order, bool) {

	cacher.Lock()
	defer cacher.Unlock()

	for _, order := range cacher.orders {
		if order.TX == tx {
			order.CommitBlock = block
			order.CommitTime = time
			order.Status = sensors.StatusRunning
			return order, true
		}
	}

	return nil, false
}
func (cacher *cacherImpl) Confirm(block int64, time time.Time) (timeout []*sensors.Order, confirmed []*sensors.Order) {

	cacher.Lock()
	defer cacher.Unlock()

	var orders []*sensors.Order

	for _, order := range cacher.orders {

		if order.Status == sensors.StatusPending && block-order.PendingBlock > cacher.timeoutBlocks {
			timeout = append(timeout, order)
			continue
		}

		if order.Status == sensors.StatusRunning && block-order.CommitBlock > cacher.confirmBlocks {
			confirmed = append(confirmed, order)
			continue
		}

		orders = append(orders, order)
	}

	cacher.orders = orders

	return
}
func (cacher *cacherImpl) Pending() (*sensors.Order, bool) {

	cacher.Lock()
	defer cacher.Unlock()

	if len(cacher.orders) == 0 {
		return nil, false
	}

	order := cacher.orders[len(cacher.orders)-1]

	if order.Status == sensors.StatusPending {
		return order, true
	}

	return nil, false
}
func (cacher *cacherImpl) Pend(order *sensors.Order) {
	cacher.Lock()
	defer cacher.Unlock()

	cacher.orders = append(cacher.orders, order)
}

func init() {
	sensors.RegisterCacher("memory-cacher", New)
}
