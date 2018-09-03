package storage

import (
	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/slf4go"
	"github.com/dynamicgo/xorm-decorator"
	"github.com/go-xorm/xorm"
	sensors "github.com/laplacenetwork/eth-sensors"
)

type storageImpl struct {
	slf4go.Logger
	engine *xorm.Engine
}

// New .
func New(config config.Config) (sensors.OrderStorage, error) {
	return NewDBStorage(
		config.Get("database", "driver").String("sqlite3"),
		config.Get("database", "source").String("../../.build/sensor.db"),
	)
}

// NewDBStorage create new database order storage
func NewDBStorage(driver, source string) (sensors.OrderStorage, error) {

	engine, err := xorm.NewEngine(driver, source)

	if err != nil {
		return nil, err
	}

	storage := &storageImpl{
		Logger: slf4go.Get("storage"),
		engine: engine,
	}

	return storage, nil
}

func (storage *storageImpl) Unconfirmed() ([]*sensors.Order, error) {

	orders := make([]*sensors.Order, 0)

	err := storage.engine.Where(
		`"status" = ? or "status" = ?`,
		sensors.StatusPending,
		sensors.StatusRunning).Find(&orders)

	return orders, err
}

func (storage *storageImpl) Save(order *sensors.Order) error {
	_, err := storage.engine.InsertOne(order)

	if decorator.DuplicateKey(storage.engine, err) {
		storage.WarnF("save order %s err for duplicate tx %s", order.TX)
		return nil
	}

	return err
}

func (storage *storageImpl) Update(order *sensors.Order) error {
	affected, err := storage.engine.Where(`"i_d" = ?`, order.ID).Update(order)

	if err != nil {
		return err
	}

	if affected == 0 {
		return sensors.ErrVersion
	}

	return nil
}

func (storage *storageImpl) Get(id string) (*sensors.Order, error) {
	var order sensors.Order

	ok, err := storage.engine.Where(`"i_d" = ?`, id).Get(&order)

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	return &order, nil
}

func init() {
	sensors.RegisterStorage("db-storage", New)
}
