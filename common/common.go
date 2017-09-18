package common

import (
	"errors"
	"fmt"
	"github.com/Symantec/proxima/config"
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/Symantec/scotty/influx/responses"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"log"
	"sort"
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
	Query(l *log.Logger, q *influxql.Query, epoch string) (
		*client.Response, error)
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

func getRawConcurrentResponses(
	endpoints []queryerType,
	queries []*influxql.Query,
	epoch string,
	logger *log.Logger) (
	responseList []*client.Response, errs []error) {
	if len(endpoints) != len(queries) {
		panic("endpoints and queries parameters must have same length")
	}
	// These are placeholders for the response and error from each influx db
	// instance.
	responseList = make([]*client.Response, len(queries))
	errs = make([]error, len(queries))

	var wg sync.WaitGroup
	for i, query := range queries {
		// Query not applicable, skip
		if query == nil {
			continue
		}
		wg.Add(1)
		go func(
			n queryerType,
			query *influxql.Query,
			responseHere **client.Response,
			errHere *error) {
			*responseHere, *errHere = n.Query(logger, query, epoch)
			wg.Done()
		}(endpoints[i],
			query,
			&responseList[i],
			&errs[i])
	}
	wg.Wait()
	return
}

func getConcurrentResponses(
	endpoints []queryerType,
	queries []*influxql.Query,
	epoch string,
	logger *log.Logger) (*client.Response, error) {
	responseList, errs := getRawConcurrentResponses(
		endpoints, queries, epoch, logger)

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
		if err == nil && response.Error() != nil {
			err = response.Error()
		}
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
	if len(influxes) == 0 {
		return nil, nil
	}
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

func (l *InfluxList) _close() error {
	if l == nil {
		return nil
	}
	var lastError lastErrorType
	for _, d := range l.instances {
		lastError.Add(d.Close())
	}
	return lastError.Error()
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
	if l == nil {
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
	if scotty.HostAndPort != "" {
		handle, err := creater(scotty.HostAndPort)
		if err != nil {
			return nil, err
		}
		return &Scotty{handle: handle}, nil
	}
	if len(scotty.Partials) != 0 {
		partials, err := newScottyPartialsForTesting(scotty.Partials, creater)
		if err != nil {
			return nil, err
		}
		return &Scotty{partials: partials}, nil
	}
	if len(scotty.Scotties) != 0 {
		scotties, err := newScottyListForTesting(scotty.Scotties, creater)
		if err != nil {
			return nil, err
		}
		return &Scotty{scotties: scotties}, nil
	}
	return nil, errors.New("Scotty must have either hostAndPort, partials, or scotties")
}

func (s *Scotty) query(
	logger *log.Logger, query *influxql.Query, epoch string) (
	*client.Response, error) {
	switch {
	case s.handle != nil:
		return s.handle.Query(query.String(), "scotty", epoch)
	case s.partials != nil:
		return s.partials.Query(logger, query, epoch)
	case s.scotties != nil:
		return s.scotties.Query(logger, query, epoch)
	}
	// Should never get here.
	panic("query should return something")
}

func (s *Scotty) _close() error {
	if s.handle != nil {
		return s.handle.Close()
	}
	if s.partials != nil {
		return s.partials.Close()
	}
	if s.scotties != nil {
		return s.scotties.Close()
	}
	return nil
}

func newScottyPartialsForTesting(
	scotties config.ScottyList, creater handleCreaterType) (
	*ScottyPartials, error) {
	if len(scotties) == 0 {
		panic("Scotty list must be non-empty")
	}
	result := &ScottyPartials{instances: make([]*Scotty, len(scotties))}
	for i := range scotties {
		var err error
		result.instances[i], err = newScottyForTesting(scotties[i], creater)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (l *ScottyPartials) _close() error {
	if l == nil {
		return nil
	}
	var lastError lastErrorType
	for _, s := range l.instances {
		lastError.Add(s.Close())
	}
	return lastError.Error()
}

func (l *ScottyPartials) query(
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
	return aggregateScottyResponses(endpoints, query, epoch, logger)
}

func newScottyListForTesting(
	scotties config.ScottyList, creater handleCreaterType) (
	*ScottyList, error) {
	if len(scotties) == 0 {
		return nil, nil
	}
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

func (l *ScottyList) _close() error {
	if l == nil {
		return nil
	}
	var lastError lastErrorType
	for _, s := range l.instances {
		lastError.Add(s.Close())
	}
	return lastError.Error()
}

func (l *ScottyList) query(
	logger *log.Logger, query *influxql.Query, epoch string) (
	*client.Response, error) {
	if l == nil {
		return responses.Merge()
	}
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

func (d *Database) _close() error {
	var lastError lastErrorType
	lastError.Add(d.influxes.Close())
	lastError.Add(d.scotties.Close())
	return lastError.Error()
}

func (d *Database) query(
	logger *log.Logger,
	query *influxql.Query,
	epoch string,
	now time.Time) (*client.Response, error) {
	if d.influxes == nil && d.scotties == nil {
		return responses.Merge()
	}
	if d.influxes == nil {
		return d.scotties.Query(logger, query, epoch)
	}
	if d.scotties == nil {
		return d.influxes.Query(logger, query, epoch, now)
	}
	var wg sync.WaitGroup
	var influxResponse *client.Response
	var influxError error
	wg.Add(1)
	go func() {
		influxResponse, influxError = d.influxes.Query(
			logger, query, epoch, now)
		wg.Done()
	}()
	var scottyResponse *client.Response
	var scottyError error
	wg.Add(1)
	go func() {
		scottyResponse, scottyError = d.scotties.Query(
			logger, query, epoch)
		wg.Done()
	}()
	wg.Wait()
	if scottyError != nil || scottyResponse.Error() != nil {
		return influxResponse, influxError
	}
	if influxError != nil || influxResponse.Error() != nil {
		return scottyResponse, scottyError
	}

	var lastError lastErrorType
	lastError.Add(influxError)
	lastError.Add(scottyError)
	if err := lastError.Error(); err != nil {
		return nil, err
	}
	return responses.MergePreferred(influxResponse, scottyResponse)
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

func (p *Proxima) names() (result []string) {
	for n := range p.dbs {
		result = append(result, n)
	}
	sort.Strings(result)
	return

}

func (p *Proxima) _close() error {
	var lastError lastErrorType
	for _, db := range p.dbs {
		lastError.Add(db.Close())
	}
	return lastError.Error()
}

func sumUpScottyResponses(
	endpoints []queryerType,
	stmt influxql.Statement,
	epoch string,
	logger *log.Logger) (result []models.Row, err error) {
	queries := make([]*influxql.Query, len(endpoints))
	query := qlutils.SingleQuery(stmt)
	for i := range queries {
		queries[i] = query
	}
	responseList, errs := getRawConcurrentResponses(
		endpoints, queries, epoch, logger)

	// If we get an error from any scotty, we might give a wrong answer
	// so the best we can do is error out.
	for i := range errs {
		if errs[i] != nil {
			return nil, errs[i]
		}
		if err := responseList[i].Error(); err != nil {
			return nil, err
		}
	}
	rowsFromResponses := make([][]models.Row, len(responseList))
	for i := range rowsFromResponses {
		rowsFromResponses[i], err = responses.ExtractRows(responseList[i])
		if err != nil {
			return
		}
	}
	return responses.SumRowsTogether(rowsFromResponses...)
}

func aggregateScottyStmtResponses(
	endpoints []queryerType,
	stmt influxql.Statement,
	epoch string,
	logger *log.Logger) (result client.Result, err error) {
	aggregationType, err := qlutils.AggregationType(stmt)
	if err != nil {
		return
	}
	if aggregationType == "mean" {
		var sumStmt, cntStmt influxql.Statement
		sumStmt, err = qlutils.WithAggregationType(stmt, "sum")
		if err != nil {
			return
		}
		cntStmt, err = qlutils.WithAggregationType(stmt, "count")
		if err != nil {
			return
		}
		var sumRows, cntRows []models.Row
		sumRows, err = sumUpScottyResponses(
			endpoints,
			sumStmt,
			epoch,
			logger)
		if err != nil {
			return
		}
		cntRows, err = sumUpScottyResponses(
			endpoints,
			cntStmt,
			epoch,
			logger)
		if err != nil {
			return
		}
		var meanRows []models.Row
		meanRows, err = responses.DivideRows(
			sumRows, cntRows, []string{"time", "mean"})
		if err != nil {
			return
		}
		return client.Result{Series: meanRows}, nil
	} else {
		var rows []models.Row
		rows, err = sumUpScottyResponses(
			endpoints,
			stmt,
			epoch,
			logger)
		if err != nil {
			return
		}
		return client.Result{Series: rows}, nil
	}

}

func aggregateScottyResponses(
	endpoints []queryerType,
	query *influxql.Query,
	epoch string,
	logger *log.Logger) (*client.Response, error) {
	var results []client.Result
	for _, stmt := range query.Statements {
		result, err := aggregateScottyStmtResponses(
			endpoints, stmt, epoch, logger)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return &client.Response{Results: results}, nil
}
