package common

import (
	"fmt"
	"github.com/Symantec/proxima/config"
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/Symantec/scotty/influx/responses"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"log"
	"sync"
	"time"
)

type lastErrorType struct {
	err error
}

func (l *lastErrorType) Add(err error) {
	if err != nil {
		l.err = err
	}
}

func (l *lastErrorType) Error() error {
	return l.err
}

type queryerType interface {
	Query(q, epoch string) (*client.Response, error)
}

type handleType interface {
	Query(queryStr, database, epoch string) (*client.Response, error)
	Close() error
}

type influxHandleType struct {
	cl client.Client
}

func (q *influxHandleType) Query(queryStr, database, epoch string) (
	*client.Response, error) {
	aQuery := client.NewQuery(queryStr, database, epoch)
	return q.cl.Query(aQuery)
}

func (q *influxHandleType) Close() error {
	return q.cl.Close()
}

type handleCreaterType func(addr string) (handleType, error)

func influxCreateHandle(addr string) (handleType, error) {
	cl, err := client.NewHTTPClient(client.HTTPConfig{Addr: addr})
	if err != nil {
		return nil, err
	}
	return &influxHandleType{cl: cl}, nil
}

func getConcurrentResponses(
	endpoints []queryerType,
	queries []*influxql.Query,
	epoch string,
	logger *log.Logger) (
	*client.Response, error) {
	if len(endpoints) != len(queries) {
		panic("endpoints and queries parameters must have same length")
	}
	// These are placeholders for the response and error from each influx db
	// instance.
	responseList := make([]*client.Response, len(queries))
	errs := make([]error, len(queries))

	var wg sync.WaitGroup
	for i, query := range queries {
		// Query not applicable, skip
		if query == nil {
			continue
		}
		wg.Add(1)
		go func(
			n queryerType,
			query string,
			responseHere **client.Response,
			errHere *error) {
			*responseHere, *errHere = n.Query(query, epoch)
			wg.Done()
		}(endpoints[i],
			query.String(),
			&responseList[i],
			&errs[i])
	}
	wg.Wait()

	// These will be the responses from influx servers that we merge
	var responsesToMerge []*client.Response

	// In case none of the responses are viable, report this error
	// back to client
	var lastErrorEncountered error

	for i := range queries {
		if queries[i] == nil {
			continue
		}
		err := errs[i]
		response := responseList[i]
		if err == nil {
			responsesToMerge = append(responsesToMerge, response)
		} else {
			if logger != nil {
				logger.Println(err)
			}
			lastErrorEncountered = err
		}
	}
	// errors but no viable responses
	if len(responsesToMerge) == 0 && lastErrorEncountered != nil {
		return nil, lastErrorEncountered
	}
	return responses.Merge(responsesToMerge...)
}

func newInfluxForTesting(
	influx config.Influx, creater handleCreaterType) (*Influx, error) {
	handle, err := creater(influx.HostAndPort)
	if err != nil {
		return nil, err
	}
	return &Influx{data: influx, handle: handle}, nil
}

func newInfluxListForTesting(
	influxes config.InfluxList, creater handleCreaterType) (
	*InfluxList, error) {
	influxes = influxes.Order()
	result := &InfluxList{instances: make([]*Influx, len(influxes))}
	for i := range influxes {
		var err error
		result.instances[i], err = newInfluxForTesting(influxes[i], creater)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (l *InfluxList) splitQuery(
	query *influxql.Query, now time.Time) (
	splitQueries []*influxql.Query,
	err error) {
	if len(l.instances) == 0 {
		return
	}
	result := make([]*influxql.Query, len(l.instances))
	for i := range result {
		min := l.minTime(i, now)
		// Query up to the present for each backend. This way if
		// an influx instance with finer grained data goes down,
		// proxima can use an influx instance with courser grained
		// data to fill in the missing times.
		result[i], err = qlutils.QuerySetTimeRange(query, min, now)
		if err != nil {
			return
		}
	}
	return result, nil
}

func (l *InfluxList) minTime(i int, now time.Time) time.Time {
	return now.Add(-l.instances[i].data.Duration)
}

func (l *InfluxList) query(
	logger *log.Logger, query *influxql.Query, epoch string, now time.Time) (
	*client.Response, error) {
	if len(l.instances) == 0 {
		return responses.Merge()
	}
	querySplits, err := l.splitQuery(query, now)
	if err != nil {
		return nil, err
	}
	endpoints := make([]queryerType, len(l.instances))
	for i := range endpoints {
		endpoints[i] = l.instances[i]
	}
	return getConcurrentResponses(endpoints, querySplits, epoch, logger)
}

func newScottyForTesting(
	scotty config.Scotty, creater handleCreaterType) (*Scotty, error) {
	handle, err := creater(scotty.HostAndPort)
	if err != nil {
		return nil, err
	}
	return &Scotty{data: scotty, handle: handle}, nil
}

func newScottyListForTesting(
	scotties config.ScottyList, creater handleCreaterType) (
	*ScottyList, error) {
	result := &ScottyList{instances: make([]*Scotty, len(scotties))}
	for i := range scotties {
		var err error
		result.instances[i], err = newScottyForTesting(scotties[i], creater)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (l *ScottyList) query(
	logger *log.Logger, query *influxql.Query, epoch string) (
	*client.Response, error) {
	endpoints := make([]queryerType, len(l.instances))
	for i := range endpoints {
		endpoints[i] = l.instances[i]
	}
	queries := make([]*influxql.Query, len(l.instances))
	for i := range queries {
		queries[i] = query
	}
	return getConcurrentResponses(endpoints, queries, epoch, logger)
}

func newDatabaseForTesting(
	db config.Database, creater handleCreaterType) (*Database, error) {
	result := &Database{name: db.Name}
	var err error
	result.influxes, err = newInfluxListForTesting(db.Influxes, creater)
	if err != nil {
		return nil, err
	}
	result.scotties, err = newScottyListForTesting(db.Scotties, creater)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *Database) query(
	logger *log.Logger,
	query *influxql.Query,
	epoch string,
	now time.Time) (*client.Response, error) {
	return nil, nil
}

func newProximaForTesting(
	proxima config.Proxima, creater handleCreaterType) (*Proxima, error) {
	result := &Proxima{dbs: make(map[string]*Database)}
	for _, dbSpec := range proxima.Dbs {
		db, err := newDatabaseForTesting(dbSpec, creater)
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
