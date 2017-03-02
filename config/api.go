package config

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"time"
)

type Influx struct {
	// http://someHost.com:1234.
	HostAndPort string `yaml:"hostAndPort"`
	// The duration in this instance's retention policy
	Duration time.Duration `yaml:"duration"`
	// The influx Database to use
	Database string `yaml:"database"`
}

func (i *Influx) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type influxFields Influx
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*influxFields)(i))
}

// InfluxList instances are to be treated as immutable
type InfluxList []Influx

// Order returns an InfluxList like this one but ordered by duration
// in descending order
func (i InfluxList) Order() InfluxList {
	result := make(InfluxList, len(i))
	copy(result, i)
	orderInfluxes(result)
	return result
}

type Scotty struct {
	// http://someHost.com:1234.
	HostAndPort string `yaml:"hostAndPort"`
}

func (s *Scotty) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type scottyFields Scotty
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*scottyFields)(s))
}

// ScottyList instances are to be treated as immutable
type ScottyList []Scotty

type Database struct {
	Name     string     `yajml:"name"`
	Influxes InfluxList `yaml:"influxes"`
	Scotties ScottyList `yaml:"scotties"`
}

func (d *Database) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type databaseFields Database
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*databaseFields)(d))
}

type Proxima struct {
	Dbs []Database `yaml:"databases"`
}

func (p *Proxima) Reset() {
	*p = Proxima{}
}

func (p *Proxima) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type proximaFields Proxima
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*proximaFields)(p))
}
