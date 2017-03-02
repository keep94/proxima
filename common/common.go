package common

import (
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/Symantec/scotty/influx/responses"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"log"
	"sync"
	"time"
)

type queryerType interface {
	Query(q, epoch string) (*client.Response, error)
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

func (d *Database) query(
	logger *log.Logger,
	query *influxql.Query,
	epoch string,
	now time.Time) (*client.Response, error) {
	return nil, nil
}
