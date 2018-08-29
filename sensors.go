package sensors

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dynamicgo/slf4go"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/orm"
)

// Errors
var (
	ErrVersion       = errors.New("order version error")
	ErrWatcherExists = errors.New("watcher exists")
)

// Status .
type Status string

// Status .
var (
	StatusCreated = Status("CREATED")
	StatusPending = Status("PENDING")
	StatusRunning = Status("RUNNING")
	StatusSucceed = Status("SUCCEED")
	StatusFailed  = Status("FAILED")
)

// Order the eth tx order
type Order struct {
	ID           string    `xorm:"pk"`
	TX           string    `xorm:"index"`
	PendingBlock int64     `xorm:""`
	CommitBlock  int64     `xorm:""`
	ConfirmBlock int64     `xorm:""`
	Status       Status    `xorm:"index"`
	CreateTime   time.Time `xorm:"created"`
	PendingTime  time.Time `xorm:""`
	CommitTime   time.Time `xorm:""`
	ConfirmTime  time.Time `xorm:""`
	From         string    `xorm:"index"`
	To           string    `xorm:"index"`
	Value        string    `xorm:"default('0x0')"`
	Code         string    `xorm:""`
	GasLimits    string    `xorm:""`
	GasPrice     string    `xorm:""`
}

// TableName .
func (table *Order) TableName() string {
	return "eth_sensors_order"
}

// Watcher the eth event watcher managed by sensors
type Watcher struct {
	ID      string `xorm:"pk"`     // watcher id
	Name    string `xorm:"index"`  // watcher name
	Key     string `xorm:"unique"` // watcher unique key provider by notifier
	Address string `xorm:"index"`  // watched address
}

// TableName .
func (table *Watcher) TableName() string {
	return "eth_sensors_watcher"
}

// Sensor The eth tx detect service
type Sensor interface {
	// create a new watcher with config
	New(watcher *Watcher) (id string, err error)
	// delete watcher by watcher key
	Delete(key string) (err error)
	// list the register watcher
	List(page orm.Page) ([]*Watcher, int64, error)
}

func init() {
	orm.RegisterWithName("eth-sensors", func() []interface{} {
		return []interface{}{
			new(Watcher), new(Order),
		}
	})
}

// Notifier the eth tx event notifier
type Notifier interface {
	// notify order status changed
	Notify(receiver *Watcher, order *Order) error
}

// NotifierFunc .
type NotifierFunc func(receiver *Watcher, order *Order) error

// Notify implement Notifier
func (fn NotifierFunc) Notify(receiver *Watcher, order *Order) error {
	return fn(receiver, order)
}

// OrderStorage .
type OrderStorage interface {
	Save(order *Order) error
	Update(order *Order) error
	Unconfirmed() ([]*Order, error)
}

// OrderCacher .
type OrderCacher interface {
	Cache([]*Order)                                                             // load unconfirmed  orders
	Mint(tx string, block int64, time time.Time) (*Order, bool)                 // mint cached order with tx string
	Confirm(block int64, time time.Time) (timeout []*Order, confirmed []*Order) // confirm orders
	Pending() (*Order, bool)                                                    // pending order number
	Pend(order *Order)
}

// NotifierF notifier factory
type NotifierF func(config config.Config) (Notifier, error)

// CoreF sensors factory
type CoreF func(config config.Config, plugin *Plugin) (Sensor, error)

// OrderCacherF OrderCacher factory
type OrderCacherF func(config config.Config) (OrderCacher, error)

// OrderStorageF OrderStorage factory
type OrderStorageF func(config config.Config) (OrderStorage, error)

// Plugin sensors plugin object
type Plugin struct {
	slf4go.Logger
	NotifierCreator     NotifierF
	sensorsCreator      CoreF
	OrderStorageCreator OrderStorageF
	OrderCacherCreator  OrderCacherF
}

var plugin *Plugin
var once sync.Once

func initPlugin() {
	plugin = &Plugin{
		Logger: slf4go.Get("eth-detechor-register"),
	}
}

// RegisterNotifier .
func RegisterNotifier(name string, notifier NotifierF) {
	once.Do(initPlugin)

	plugin.DebugF("create notifier: %s", name)

	plugin.NotifierCreator = notifier
}

// RegisterSensor .
func RegisterSensor(name string, sensors CoreF) {
	once.Do(initPlugin)

	plugin.DebugF("create sensors: %s", name)

	plugin.sensorsCreator = sensors
}

// RegisterCacher .
func RegisterCacher(name string, cacherF OrderCacherF) {
	once.Do(initPlugin)

	plugin.DebugF("create sensors: %s", name)

	plugin.OrderCacherCreator = cacherF
}

// RegisterStorage .
func RegisterStorage(name string, cacherF OrderStorageF) {
	once.Do(initPlugin)

	plugin.DebugF("create sensors: %s", name)

	plugin.OrderStorageCreator = cacherF
}

// New create sensors
func New(config config.Config, options ...Option) (Sensor, error) {

	p := &Plugin{}

	if plugin != nil {
		p.NotifierCreator = plugin.NotifierCreator
		p.OrderCacherCreator = plugin.OrderCacherCreator
		p.sensorsCreator = plugin.sensorsCreator
		p.OrderStorageCreator = plugin.OrderStorageCreator
	}

	for _, option := range options {
		option(p)
	}

	if p.sensorsCreator == nil {
		return nil, fmt.Errorf("expect import sensors implement")
	}

	if p.NotifierCreator == nil {
		return nil, fmt.Errorf("expect import notifier implement")
	}

	if p.OrderStorageCreator == nil {
		return nil, fmt.Errorf("expect import order storage implement")
	}

	if p.OrderCacherCreator == nil {
		return nil, fmt.Errorf("expect import order cacher implement")
	}

	return p.sensorsCreator(config, p)
}

// Option .
type Option func(plugin *Plugin)

// WithNotifier .
func WithNotifier(notifier NotifierF) Option {
	return func(plugin *Plugin) {
		plugin.NotifierCreator = notifier
	}
}
