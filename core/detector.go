package core

import (
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/dynamicgo/go-config-extend"

	"github.com/dynamicgo/fixed"

	"github.com/dynamicgo/xorm-decorator"
	"github.com/openzknetwork/ethgo/erc20"
	"github.com/openzknetwork/ethgo/rpc"
	"github.com/openzknetwork/indexer"

	"github.com/bwmarrin/snowflake"
	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/orm"
	"github.com/dynamicgo/slf4go"
	"github.com/go-xorm/xorm"
	xormrediscache "github.com/go-xorm/xorm-redis-cache"
	sensors "github.com/laplacenetwork/eth-sensors"
	ethfetcher "github.com/openzknetwork/indexer/eth"
)

type sensorsImpl struct {
	slf4go.Logger
	db       *xorm.Engine
	snode    *snowflake.Node
	indexer  indexer.Indexer
	cacher   sensors.OrderCacher
	storage  sensors.OrderStorage
	notifier sensors.Notifier
	ethnode  *rpc.Client
}

// New create the sensors engine service
func New(config config.Config, plugin *sensors.Plugin) (sensors.Sensor, error) {

	impl := &sensorsImpl{
		Logger: slf4go.Get("sensors"),
	}

	snode, err := snowflake.NewNode(int64(config.Get("snode").Int(4)))

	if err != nil {
		impl.ErrorF("create snode err: 5s", err)
		return nil, err
	}

	impl.snode = snode

	if err := impl.createDB(config); err != nil {
		return nil, err
	}

	if err := impl.createIndexer(config); err != nil {
		return nil, err
	}

	storageConfig, err := extend.SubConfig(config, "storage")

	if err != nil {
		return nil, err
	}

	storage, err := plugin.OrderStorageCreator(storageConfig)

	if err != nil {
		return nil, err
	}

	impl.storage = storage

	cacherConfig, err := extend.SubConfig(config, "cacher")

	if err != nil {
		return nil, err
	}

	cacher, err := plugin.OrderCacherCreator(cacherConfig)

	if err != nil {
		return nil, err
	}

	impl.cacher = cacher

	notifierConfig, err := extend.SubConfig(config, "notifier")

	if err != nil {
		return nil, err
	}

	notifier, err := plugin.NotifierCreator(notifierConfig)

	if err != nil {
		return nil, err
	}

	impl.notifier = notifier

	orders, err := impl.storage.Unconfirmed()

	if err != nil {
		return nil, err
	}

	impl.DebugF("load unconfirmed orders %d", len(orders))

	impl.cacher.Cache(orders)

	go impl.indexer.Run()

	return impl, nil
}

func (d *sensorsImpl) createIndexer(config config.Config) error {

	ethnode := config.Get("ethnode").String("http://localhost:8545")

	fetcher := ethfetcher.New(ethnode, ethfetcher.HandleFunc(d.fetchHandler))

	idx, err := indexer.New(config, fetcher)

	if err != nil {
		d.ErrorF("create indexer err: %s", err)
		return err
	}

	d.indexer = idx
	d.ethnode = rpc.NewClient(ethnode)

	return nil
}

func (d *sensorsImpl) fetchHandler(block *rpc.Block) error {
	blockNumber, _ := strconv.ParseUint(strings.TrimPrefix(block.Number, "0x"), 16, 64)

	timestamp, _ := strconv.ParseInt(strings.TrimPrefix(block.Timestamp, "0x"), 16, 64)

	blockTime := time.Unix(timestamp, 0)

	for _, tx := range block.Transactions {
		// d.DebugF("handle tx(%s) ", tx.Hash)

		err := d.TX(tx, int64(blockNumber), blockTime)

		if err != nil {
			d.ErrorF("handle tx(%s) err %s", tx.Hash, err)
			return err
		}

		// d.DebugF("handle tx(%s) -- success", tx.Hash)
	}

	d.DebugF("handle block(%s)", block.Hash)

	if err := d.Block(block, int64(blockNumber), blockTime); err != nil {
		d.ErrorF("handle block(%s) err %s", block.Hash, err)
		return err
	}

	d.DebugF("handle block(%s) -- success", block.Hash)

	return nil
}

func (d *sensorsImpl) getWatchers(order *sensors.Order) ([]*sensors.Watcher, error) {

	to, err := d.erc20Watcher(order)

	if err != nil {
		return nil, err
	}

	if to == "" {
		to = order.To
	}

	watchers := make([]*sensors.Watcher, 0)

	d.DebugF("find watcher for %s or %s", order.From, to)

	err = d.db.Where(`("address" = ? or "address" = ?) and "e_r_c20" = ?`, order.From, to, false).Find(&watchers)

	if err != nil {
		d.DebugF("find watcher for %s or %s err: %s", order.From, to, err)
		return nil, err
	}

	return watchers, nil
}

func (d *sensorsImpl) erc20Watcher(order *sensors.Order) (string, error) {

	watcher := new(sensors.Watcher)

	ok, err := d.db.Where(`"address" = ? and "e_r_c20" = ?`, order.To, true).Get(watcher)

	if err != nil {
		return "", err
	}

	if !ok {
		return "", nil
	}

	d.DebugF("find erc20 call %s", order.TX)

	code := strings.TrimPrefix(order.Code, "0x")

	if !strings.HasPrefix(code, erc20.TransferID) {
		d.WarnF("check contract %s call %s, is not a transfer call, %s", order.To, order.TX, code)
		return "", nil
	}

	code = strings.TrimPrefix(code, erc20.TransferID)

	if len(code) != 128 {
		d.WarnF("handle unknown contract transfer method: %s", order.TX)
		return "", nil
	}

	to := "0x" + code[24:64]

	d.DebugF("erc20 %s transfer to %s", order.TX, to)

	return to, nil
}

func (d *sensorsImpl) TX(tx *rpc.Transaction, blockNumber int64, blockTime time.Time) error {

	order := &sensors.Order{
		ID:           "O_" + d.snode.Generate().String(),
		TX:           tx.Hash,
		PendingBlock: blockNumber,
		CommitBlock:  blockNumber,
		ConfirmBlock: -1,
		Status:       sensors.StatusRunning,
		PendingTime:  blockTime,
		CreateTime:   blockTime,
		CommitTime:   blockTime,
		From:         tx.From,
		To:           tx.To,
		Value:        tx.Value,
		GasLimits:    tx.Gas,
		GasPrice:     tx.GasPrice,
		Code:         tx.Input,
	}

	d.DebugF("try get tx %s watcher", order.TX)

	watchers, err := d.getWatchers(order)

	if err != nil {
		return err
	}

	if len(watchers) == 0 {
		// d.DebugF("no watcher for tx %s", tx.Hash)
		return nil
	}

	d.DebugF("find watchers(%d) for tx %s", len(watchers), tx.Hash)

	gas, err := fixed.FromHex(tx.Gas, 0)

	if err != nil {
		d.ErrorF("parse tx %s gas %s err: %s", tx.Hash, tx.Gas, err)
		return nil
	}

	gasPrice, err := fixed.FromHex(tx.GasPrice, 0)

	if err != nil {
		d.ErrorF("parse tx %s gas price %s err: %s", tx.Hash, tx.GasPrice, err)
		return nil
	}

	gasLimits := new(big.Int).Quo(gas.ValueBigInteger(), gasPrice.ValueBigInteger())

	order.GasLimits = fixed.NewWithBigint(gasLimits, 0).HexValue()

	d.DebugF("notify watchers(%d) for tx %s", len(watchers), tx.Hash)

	for _, watcher := range watchers {
		d.DebugF("notify watcher %s for tx %s", watcher.Address, tx.Hash)
		if err := d.notifier.Notify(watcher, order); err != nil {
			d.ErrorF("notify tx %s order to watcher %s err: %s", tx.Hash, watcher.Key, err)
			return err
		}
	}

	if err := d.storage.Save(order); err != nil {

		d.ErrorF("save tx %s order err: %s", tx.Hash, err)
		return err
	}

	d.cacher.Pend(order)

	return nil
}

func (d *sensorsImpl) orderRecipt(tx string) (bool, error) {
	recipt, err := d.ethnode.GetTransactionReceipt(tx)

	if err != nil {
		return false, err
	}

	if recipt.Status == "0x0" {
		return false, nil
	}

	return true, nil
}

func (d *sensorsImpl) recache(timeout, confirmed []*sensors.Order) {
	for _, order := range timeout {

		order.Status = sensors.StatusPending
	}

	for _, order := range confirmed {

		order.Status = sensors.StatusRunning
	}

	d.cacher.Cache(append(timeout, confirmed...))
}

func (d *sensorsImpl) Block(block *rpc.Block, blockNumber int64, blockTime time.Time) error {

	timeout, confirmed := d.cacher.Confirm(blockNumber, blockTime)

	for _, order := range timeout {
		d.InfoF("timeout order %s with tx %s block %d", order.ID, order.TX, blockNumber)

		order.Status = sensors.StatusFailed

		order.ConfirmBlock = blockNumber
		order.ConfirmTime = blockTime
	}

	for _, order := range confirmed {
		d.InfoF("confirmed order %s with tx %s block %d", order.ID, order.TX, blockNumber)
		ok, err := d.orderRecipt(order.TX)

		if err != nil {
			d.recache(timeout, confirmed)
			return err
		}

		if ok {
			order.Status = sensors.StatusSucceed
		} else {
			order.Status = sensors.StatusFailed
		}

		order.ConfirmBlock = blockNumber
		order.ConfirmTime = blockTime
	}

	orders := append(timeout, confirmed...)

	for _, order := range orders {
		watchers, err := d.getWatchers(order)

		if err != nil {
			d.ErrorF("notify tx %s completed err: %s", order.TX, err)
			d.recache(timeout, confirmed)
			return err
		}

		d.DebugF("find watchers(%d) for tx %s to notify", len(watchers), order.TX)

		for _, watcher := range watchers {
			if err := d.notifier.Notify(watcher, order); err != nil {
				d.ErrorF("notify tx %s completed err: %s", order.TX, err)
				d.recache(timeout, confirmed)
				return err
			}
		}
	}

	for _, order := range orders {
		if err := d.storage.Update(order); err != nil {
			d.ErrorF("save order %s err: %s", order.TX, err)
			d.recache(timeout, confirmed)
			return err
		}
	}

	return nil
}

func (d *sensorsImpl) createDB(config config.Config) error {
	driver := config.Get("database", "driver").String("sqlite3")
	source := config.Get("database", "source").String("../.build/sensors.db")

	d.DebugF("create watcher database:\nsource: %s\ndriver:%s", source, driver)

	db, err := xorm.NewEngine(driver, source)

	d.db = db

	if err != nil {
		d.ErrorF("create database for watcher err: %s", err)
		return err
	}

	if "" != config.Get("database", "redis", "addr").String("") {

		// try setup xorm cacher
		cacher := xormrediscache.NewRedisCacher(
			config.Get("database", "redis", "addr").String("localhost:6379"),
			config.Get("database", "redis", "password").String(""),
			config.Get("database", "redis", "timeout").Duration(time.Minute),
			db.Logger(),
		)

		db.SetDefaultCacher(cacher)
	}

	return err

}

func (d *sensorsImpl) New(watcher *sensors.Watcher) (id string, err error) {
	watcher.ID = "W_" + d.snode.Generate().String()

	watcher.Address = strings.ToLower(watcher.Address)

	_, err = d.db.InsertOne(watcher)

	if err != nil {
		if decorator.DuplicateKey(d.db, err) {
			return "", sensors.ErrWatcherExists
		}

		return "", err
	}

	return watcher.ID, nil
}

func (d *sensorsImpl) Delete(key string) (err error) {

	_, err = d.db.Where(`"key" = ?`, key).Delete(new(sensors.Watcher))

	return err
}

func (d *sensorsImpl) List(page orm.Page) ([]*sensors.Watcher, int64, error) {

	wtachers := make([]*sensors.Watcher, 0)

	session := d.db.Limit(int(page.Size), int(page.Offset))

	if page.OrderBy != "" {
		if page.Order == orm.DESC {
			session = session.Desc(page.OrderBy)
		} else {
			session = session.Desc(page.OrderBy)
		}
	}

	c, err := session.FindAndCount(&wtachers)

	return wtachers, c, err
}

func init() {
	sensors.RegisterSensor("", New)
}
