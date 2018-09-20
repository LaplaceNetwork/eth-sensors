package db

import (
	"github.com/dynamicgo/orm"
	sensors "github.com/laplacenetwork/eth-sensors"
)

// Empty .
type Empty struct {
}

func init() {
	orm.RegisterWithName("eth-sensors", func() []interface{} {
		return []interface{}{
			new(sensors.Watcher), new(sensors.Order), new(sensors.ERC20),
		}
	})
}
