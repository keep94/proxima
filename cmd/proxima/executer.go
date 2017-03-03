package main

import (
	"errors"
	"github.com/Symantec/proxima/common"
	"github.com/Symantec/proxima/config"
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/influxdata/influxdb/client/v2"
	"io"
	"log"
	"sync"
	"time"
)

var (
	kInfluxTricorderPath = "/proc/influx"
)

var (
	kErrNoSuchDatabase = errors.New("No such database.")
)

type proximaType struct {
	*common.Proxima
	mu         sync.Mutex
	inUseCount int
	toBeClosed bool
}

func (p *proximaType) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inUseCount--
	if p.inUseCount < 0 {
		panic("Closed too many times")
	}
	if p.inUseCount == 0 && p.toBeClosed {
		return p.Proxima.Close()
	}
	return nil
}

func (p *proximaType) open() *proximaType {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inUseCount++
	return p
}

func (p *proximaType) requestClose() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.toBeClosed = true
	if p.inUseCount == 0 {
		p.Proxima.Close()
	}
}

// executerType executes queries across multiple influx db instances.
// executerType instances are safe to use with multiple goroutines
type executerType struct {
	mu      sync.Mutex
	proxima *proximaType
}

// newExecuter returns a new instance with no configuration. Querying it
// will always yield errNoBackends.
func newExecuter() *executerType {
	proxima, err := common.NewProxima(config.Proxima{})
	if err != nil {
		panic(err)
	}
	return &executerType{
		proxima: &proximaType{
			Proxima: proxima,
		},
	}
}

func (e *executerType) set(p *common.Proxima) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.proxima.requestClose()
	e.proxima = &proximaType{Proxima: p}
}

func (e *executerType) get() *proximaType {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.proxima.open()
}

// SetupWithStream sets up this instance with config file contents in r.
func (e *executerType) SetupWithStream(r io.Reader) error {
	var proximaConfig config.Proxima
	if err := yamlutil.Read(r, &proximaConfig); err != nil {
		return err
	}
	proxima, err := common.NewProxima(proximaConfig)
	if err != nil {
		return err
	}
	if err := registerMetrics(&proximaConfig); err != nil {
		return err
	}
	e.set(proxima)
	return nil
}

func registerMetrics(proximaConfig *config.Proxima) error {
	tricorder.UnregisterPath(kInfluxTricorderPath)
	/*
		influxDir, err := tricorder.RegisterDirectory(kInfluxTricorderPath)
		if err != nil {
			return err
		}
	*/
	// TODO
	return nil
}

// We have to compare the error strings because the RPC call to scotty
// prevents the error from scotty from being compared directly.
// TODO
func isUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == qlutils.ErrUnsupported.Error()
}

func (e *executerType) Names() []string {
	p := e.get()
	defer p.Close()
	return p.Names()
}

// Query runs a query against multiple influx db instances merging the results
// Query uses the logger instance to report any influx instances that are
// down.
func (e *executerType) Query(
	logger *log.Logger, queryStr, database, epoch string) (
	*client.Response, error) {
	p := e.get()
	defer p.Close()
	now := time.Now()
	query, err := qlutils.NewQuery(queryStr, now)
	if err != nil {
		return nil, err
	}
	db := p.ByName(database)
	if db == nil {
		return nil, kErrNoSuchDatabase
	}
	return db.Query(logger, query, epoch, now)
}
