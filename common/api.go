package common

import (
	"fmt"
	"github.com/Symantec/proxima/config"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"log"
	"time"
)

type Influx struct {
	data   config.Influx
	handle client.Client
}

func NewInflux(influx config.Influx) (*Influx, error) {
	result := &Influx{data: influx}
	var err error
	result.handle, err = client.NewHTTPClient(client.HTTPConfig{
		Addr: influx.HostAndPort,
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *Influx) Query(queryStr, epoch string) (*client.Response, error) {
	aQuery := client.NewQuery(queryStr, d.data.Database, epoch)
	return d.handle.Query(aQuery)
}

type InfluxList struct {
	instances []*Influx
}

func NewInfluxList(influxes config.InfluxList) (*InfluxList, error) {
	influxes = influxes.Order()
	result := &InfluxList{instances: make([]*Influx, len(influxes))}
	for i := range influxes {
		var err error
		result.instances[i], err = NewInflux(influxes[i])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (l *InfluxList) Query(
	logger *log.Logger, query *influxql.Query, epoch string, now time.Time) (
	*client.Response, error) {
	return l.query(logger, query, epoch, now)
}

type Scotty struct {
	data   config.Scotty
	handle client.Client
}

func NewScotty(scotty config.Scotty) (*Scotty, error) {
	result := &Scotty{data: scotty}
	var err error
	result.handle, err = client.NewHTTPClient(client.HTTPConfig{
		Addr: scotty.HostAndPort,
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Scotty) Query(queryStr, epoch string) (*client.Response, error) {
	aQuery := client.NewQuery(queryStr, "scotty", epoch)
	return s.handle.Query(aQuery)
}

type ScottyList struct {
	instances []*Scotty
}

func NewScottyList(scotties config.ScottyList) (*ScottyList, error) {
	result := &ScottyList{instances: make([]*Scotty, len(scotties))}
	for i := range scotties {
		var err error
		result.instances[i], err = NewScotty(scotties[i])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (l *ScottyList) Query(
	logger *log.Logger, query *influxql.Query, epoch string) (
	*client.Response, error) {
	return l.query(logger, query, epoch)
}

type Database struct {
	name     string
	influxes *InfluxList
	scotties *ScottyList
}

func NewDatabase(db config.Database) (*Database, error) {
	result := &Database{name: db.Name}
	var err error
	result.influxes, err = NewInfluxList(db.Influxes)
	if err != nil {
		return nil, err
	}
	result.scotties, err = NewScottyList(db.Scotties)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *Database) Name() string {
	return d.name
}

func (d *Database) Query(
	logger *log.Logger,
	query *influxql.Query,
	epoch string,
	now time.Time) (*client.Response, error) {
	return d.query(logger, query, epoch, now)
}

type Proxima struct {
	dbs map[string]*Database
}

func NewProxima(proxima config.Proxima) (*Proxima, error) {
	result := &Proxima{dbs: make(map[string]*Database)}
	for _, dbSpec := range proxima.Dbs {
		db, err := NewDatabase(dbSpec)
		if err != nil {
			return nil, err
		}
		if _, ok := result.dbs[db.Name()]; ok {
			return nil, fmt.Errorf("Duplicate database name: %s", db.Name())
		}
		result.dbs[db.Name()] = db
	}
	return result, nil
}

func (p *Proxima) ByName(name string) *Database {
	return p.dbs[name]
}
