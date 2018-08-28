package core

import (
	"io/ioutil"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/openzknetwork/ethgo/rpc"
	"github.com/openzknetwork/ethgo/tx"

	"github.com/openzknetwork/ethgo"

	"github.com/dynamicgo/orm"
	"github.com/go-xorm/xorm"

	"github.com/stretchr/testify/require"

	goconfig "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/go-config/source/file"
	sensors "github.com/laplacenetwork/eth-sensors"
	_ "github.com/laplacenetwork/eth-sensors/cacher"
	_ "github.com/laplacenetwork/eth-sensors/storage"
	"github.com/openzknetwork/ethgo/keystore"
)

var sensor sensors.Sensor
var key *keystore.Key
var notifier *testNotifier
var client *rpc.Client

type testNotifier struct {
	sync.Mutex
	C map[string]chan *sensors.Order
}

func (notifier *testNotifier) Notify(receiver *sensors.Watcher, order *sensors.Order) error {
	notifier.Lock()
	c, ok := notifier.C[strings.ToLower(receiver.Key)]
	notifier.Unlock()

	if ok {
		c <- order
	} else {
		println("unknown watcher", receiver.Key)
	}

	return nil
}

func (notifier *testNotifier) Listen(key string) <-chan *sensors.Order {
	notifier.Lock()
	defer notifier.Unlock()

	c := make(chan *sensors.Order, 1000)

	notifier.C[key] = c

	return c
}

func init() {
	config := goconfig.NewConfig()

	err := config.Load(file.NewSource(file.WithPath("../../conf/sensor.json")))

	if err != nil {
		panic(err)
	}

	driver := config.Get("database", "driver").String("sqlite3")
	source := config.Get("database", "source").String("../.build/sensors.db")

	db, err := xorm.NewEngine(driver, source)

	if err != nil {
		panic(err)
	}

	if err := orm.Sync(db); err != nil {
		panic(err)
	}

	client = rpc.NewClient(config.Get("ethnode").String("http://localhost:8545"))

	sensors.RegisterNotifier("test-notifier", func(config goconfig.Config) (sensors.Notifier, error) {
		notifier = &testNotifier{
			C: make(map[string]chan *sensors.Order),
		}

		return notifier, nil
	})

	sensor, err = sensors.New(config)

	if err != nil {
		panic(err)
	}

	// load keystore

	buff, err := ioutil.ReadFile("../../conf/keystore/1.json")

	if err != nil {
		panic(err)
	}

	key, err = keystore.ReadKeyStore(buff, "test")

	if err != nil {
		panic(err)
	}

}

func evalGasPrice() (*ethgo.Value, error) {
	gasPrice, err := client.SuggestGasPrice()

	if err != nil {
		return nil, err
	}

	return ethgo.NewValue(new(big.Float).SetInt(gasPrice), ethgo.Wei), nil
}

func transTest() (string, error) {

	nonce, err := client.Nonce(key.Address)

	if err != nil {
		return "", err
	}

	val := ethgo.NewValue(big.NewFloat(0.01), ethgo.Ether)
	gasPrice, err := evalGasPrice()

	if err != nil {
		return "", err
	}

	gasLimits := big.NewInt(21000)

	rawTx := tx.NewTx(nonce, key.Address, val, gasPrice, gasLimits, nil)

	err = rawTx.Sign(key.PrivateKey)

	if err != nil {
		return "", err
	}

	data, err := rawTx.Encode()

	if err != nil {
		return "", err
	}

	return client.SendRawTransaction(data)
}

func TestRegisterWatcher(t *testing.T) {
	id, err := sensor.New(&sensors.Watcher{
		Key:     "test",
		Name:    "test",
		Address: key.Address,
	})

	if err != nil && err != sensors.ErrWatcherExists {
		require.NoError(t, err)
	}

	c := notifier.Listen("test")

	println("create watcher", id)

	tx, err := transTest()

	require.NoError(t, err)

	println(tx)

	for {
		order := <-c

		require.NotEqual(t, order.Status, sensors.StatusFailed)

		if order.Status == sensors.StatusSucceed && order.TX == tx {
			break
		}
	}

}
