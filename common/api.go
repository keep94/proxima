package common

import (
	"github.com/Symantec/proxima/config"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"log"
	"time"
)

type Influx struct {
	data   config.Influx
	handle handleType
}

func NewInflux(influx config.Influx) (*Influx, error) {
	return newInfluxForTesting(influx, influxCreateHandle)
}

func (d *Influx) Query(queryStr, epoch string) (*client.Response, error) {
	return d.handle.Query(queryStr, d.data.Database, epoch)
}

func (d *Influx) Close() error {
	return d.handle.Close()
}

type InfluxList struct {
	instances []*Influx
}

func NewInfluxList(influxes config.InfluxList) (*InfluxList, error) {
	return newInfluxListForTesting(influxes, influxCreateHandle)
}

func (l *InfluxList) Query(
	logger *log.Logger, query *influxql.Query, epoch string, now time.Time) (
	*client.Response, error) {
	return l.query(logger, query, epoch, now)
}

func (l *InfluxList) Close() error {
	var lastError lastErrorType
	for _, d := range l.instances {
		lastError.Add(d.Close())
	}
	return lastError.Error()
}

type Scotty struct {
	data   config.Scotty
	handle handleType
}

func NewScotty(scotty config.Scotty) (*Scotty, error) {
	return newScottyForTesting(scotty, influxCreateHandle)
}

func (s *Scotty) Query(queryStr, epoch string) (*client.Response, error) {
	return s.handle.Query(queryStr, "scotty", epoch)
}

func (s *Scotty) Close() error {
	return s.handle.Close()
}

type ScottyList struct {
	instances []*Scotty
}

func NewScottyList(scotties config.ScottyList) (*ScottyList, error) {
	return newScottyListForTesting(scotties, influxCreateHandle)
}

func (l *ScottyList) Query(
	logger *log.Logger, query *influxql.Query, epoch string) (
	*client.Response, error) {
	return l.query(logger, query, epoch)
}

func (l *ScottyList) Close() error {
	var lastError lastErrorType
	for _, s := range l.instances {
		lastError.Add(s.Close())
	}
	return lastError.Error()
}

type Database struct {
	name     string
	influxes *InfluxList
	scotties *ScottyList
}

func NewDatabase(db config.Database) (*Database, error) {
	return newDatabaseForTesting(db, influxCreateHandle)
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

func (d *Database) Close() error {
	var lastError lastErrorType
	lastError.Add(d.influxes.Close())
	lastError.Add(d.scotties.Close())
	return lastError.Error()
}

type Proxima struct {
	dbs map[string]*Database
}

func NewProxima(proxima config.Proxima) (*Proxima, error) {
	return newProximaForTesting(proxima, influxCreateHandle)
}

func (p *Proxima) ByName(name string) *Database {
	return p.dbs[name]
}

func (p *Proxima) Names() (result []string) {
	for n := range p.dbs {
		result = append(result, n)
	}
	return
}

func (p *Proxima) Close() error {
	var lastError lastErrorType
	for _, db := range p.dbs {
		lastError.Add(db.Close())
	}
	return lastError.Error()
}
