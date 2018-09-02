package db

import (
	"github.com/dynamicgo/orm"
	sensors "github.com/laplacenetwork/eth-sensors"
)

func init() {
	orm.RegisterWithName("eth-sensors", func() []interface{} {
		return []interface{}{
			new(sensors.Watcher), new(sensors.Order),
		}
	})
}
