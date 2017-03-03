package common

import (
	"encoding/json"
	"errors"
	"github.com/Symantec/proxima/config"
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
	"time"
)

var (
	kErrCreatingHandle = errors.New("common:Error creating handle")
)

type queryCallType struct {
	query    string
	database string
	epoch    string
}

type fakeHandleType struct {
	queryCalls    []queryCallType
	queryResponse *client.Response
	queryError    error
	closed        bool
}

func (f *fakeHandleType) WhenQueriedReturn(
	response *client.Response, err error) {
	f.queryResponse, f.queryError = response, err
}

func (f *fakeHandleType) NextQuery() (
	query, database, epoch string) {
	query = f.queryCalls[0].query
	database = f.queryCalls[0].database
	epoch = f.queryCalls[0].epoch
	length := len(f.queryCalls)
	copy(f.queryCalls, f.queryCalls[1:])
	f.queryCalls = f.queryCalls[:length-1]
	return
}

func (f *fakeHandleType) NoMoreQueries() bool {
	return len(f.queryCalls) == 0
}

func (f *fakeHandleType) Closed() bool {
	return f.closed
}

func (f *fakeHandleType) Query(queryStr, database, epoch string) (
	*client.Response, error) {
	if f.closed {
		panic("Cannot query a closed handle")
	}
	f.queryCalls = append(
		f.queryCalls,
		queryCallType{
			query:    queryStr,
			database: database,
			epoch:    epoch,
		})
	return f.queryResponse, f.queryError
}

func (f *fakeHandleType) Close() error {
	f.closed = true
	return nil
}

type handleStoreType map[string]*fakeHandleType

func (s handleStoreType) Create(addr string) (handleType, error) {
	result, ok := s[addr]
	if !ok {
		return nil, kErrCreatingHandle
	}
	return result, nil
}

func (s handleStoreType) AllClosed() bool {
	for _, h := range s {
		if !h.Closed() {
			return false
		}
	}
	return true
}

var (
	kTimeValueColumns = []string{"time", "value"}
)

func newResponse(values ...int64) *client.Response {
	realValues := make([][]interface{}, len(values)/2)
	for i := range realValues {
		realValues[i] = []interface{}{
			json.Number(strconv.FormatInt(values[2*i], 10)),
			json.Number(strconv.FormatInt(values[2*i+1], 10)),
		}
	}
	return &client.Response{
		Results: []client.Result{
			{
				Series: []models.Row{
					{
						Name:    "alpha",
						Columns: kTimeValueColumns,
						Values:  realValues,
					},
				},
			},
		},
	}

}

func TestAPI(t *testing.T) {
	Convey("Given a proxima", t, func() {
		now := time.Date(2016, 12, 1, 0, 1, 0, 0, time.UTC)
		store := handleStoreType{
			"alpha":   &fakeHandleType{},
			"bravo":   &fakeHandleType{},
			"charlie": &fakeHandleType{},
			"delta":   &fakeHandleType{},
			"echo":    &fakeHandleType{},
			"foxtrot": &fakeHandleType{},
		}
		store["alpha"].WhenQueriedReturn(newResponse(1000, 10, 1200, 11), nil)
		store["bravo"].WhenQueriedReturn(newResponse(1200, 12, 1400, 13), nil)
		store["charlie"].WhenQueriedReturn(newResponse(1400, 14, 1600, 15), nil)
		store["delta"].WhenQueriedReturn(newResponse(1400, 24, 1600, 25), nil)
		store["echo"].WhenQueriedReturn(newResponse(1600, 26, 1800, 27), nil)
		store["foxtrot"].WhenQueriedReturn(newResponse(1800, 28, 2000, 29), nil)

		influxConfigs := config.InfluxList{
			{
				HostAndPort: "charlie",
				Database:    "c",
				Duration:    time.Hour,
			},
			{
				HostAndPort: "alpha",
				Database:    "a",
				Duration:    100 * time.Hour,
			},
			{
				HostAndPort: "bravo",
				Database:    "b",
				Duration:    10 * time.Hour,
			},
		}

		scottyConfigs := config.ScottyList{
			{HostAndPort: "delta"},
			{HostAndPort: "echo"},
			{HostAndPort: "foxtrot"},
		}

		proximaConfig := config.Proxima{
			Dbs: []config.Database{
				{
					Name:     "influx",
					Influxes: influxConfigs,
				},
				{
					Name:     "scotty",
					Scotties: scottyConfigs,
				},
				{
					Name: "nothing",
				},
				{
					Name:     "both",
					Influxes: influxConfigs,
					Scotties: scottyConfigs,
				},
			},
		}
		proxima, err := newProximaForTesting(proximaConfig, store.Create)
		Convey("Close should free resources", func() {
			So(proxima.Close(), ShouldBeNil)
			So(store.AllClosed(), ShouldBeTrue)
		})
		Convey("Names should return names in alphabetical order", func() {
			names := proxima.Names()
			So(names, ShouldResemble, []string{
				"both", "influx", "nothing", "scotty"})
		})
		So(err, ShouldBeNil)
		Convey("Nothing", func() {
			db := proxima.ByName("nothing")
			So(db, ShouldNotBeNil)
			Convey("Query should return zero", func() {
				query, err := qlutils.NewQuery(
					"select mean(value) from dual where time >= now() - 5h", now)
				So(err, ShouldBeNil)
				response, err := db.Query(nil, query, "ns", now)
				So(err, ShouldBeNil)
				So(*response, ShouldBeZeroValue)
			})
		})
		Convey("Just influx", func() {
			db := proxima.ByName("influx")
			So(db, ShouldNotBeNil)

			Convey("Query going to now should work", func() {
				query, err := qlutils.NewQuery(
					"select mean(value) from dual where time >= now() - 5h", now)
				So(err, ShouldBeNil)
				response, err := db.Query(nil, query, "ns", now)
				So(err, ShouldBeNil)
				So(response, ShouldResemble, newResponse(
					1000, 10,
					1200, 12,
					1400, 14,
					1600, 15,
				))
				queryStr, database, epoch := store["alpha"].NextQuery()
				So(
					queryStr,
					ShouldEqual,
					"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z' AND time < '2016-12-01T00:01:00Z'")
				So(database, ShouldEqual, "a")
				So(epoch, ShouldEqual, "ns")
				So(store["alpha"].NoMoreQueries(), ShouldBeTrue)

				queryStr, database, epoch = store["bravo"].NextQuery()
				So(
					queryStr,
					ShouldEqual,
					"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z' AND time < '2016-12-01T00:01:00Z'")
				So(database, ShouldEqual, "b")
				So(epoch, ShouldEqual, "ns")
				So(store["bravo"].NoMoreQueries(), ShouldBeTrue)

				queryStr, database, epoch = store["charlie"].NextQuery()
				So(
					queryStr,
					ShouldEqual,
					"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T23:01:00Z' AND time < '2016-12-01T00:01:00Z'")
				So(database, ShouldEqual, "c")
				So(epoch, ShouldEqual, "ns")
				So(store["charlie"].NoMoreQueries(), ShouldBeTrue)
			})

			Convey("query stopping before now should work", func() {
				query, err := qlutils.NewQuery(
					"select mean(value) from dual where time >= now() - 120h and time < now() - 5h", now)
				So(err, ShouldBeNil)
				response, err := db.Query(nil, query, "ns", now)
				So(err, ShouldBeNil)
				So(response, ShouldResemble, newResponse(
					1000, 10,
					1200, 12,
					1400, 13,
				))
				queryStr, database, epoch := store["alpha"].NextQuery()
				So(
					queryStr,
					ShouldEqual,
					"SELECT mean(value) FROM dual WHERE time >= '2016-11-26T20:01:00Z' AND time < '2016-11-30T19:01:00Z'")
				So(database, ShouldEqual, "a")
				So(epoch, ShouldEqual, "ns")
				So(store["alpha"].NoMoreQueries(), ShouldBeTrue)

				queryStr, database, epoch = store["bravo"].NextQuery()
				So(
					queryStr,
					ShouldEqual,
					"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T14:01:00Z' AND time < '2016-11-30T19:01:00Z'")
				So(database, ShouldEqual, "b")
				So(epoch, ShouldEqual, "ns")
				So(store["bravo"].NoMoreQueries(), ShouldBeTrue)
				So(store["charlie"].NoMoreQueries(), ShouldBeTrue)
			})
		})
		Convey("Just scotty", func() {
			db := proxima.ByName("scotty")
			So(db, ShouldNotBeNil)

			Convey("Scotty query should work", func() {
				query, err := qlutils.NewQuery(
					"select mean(value) from dual where time >= now() - 5h", now)
				So(err, ShouldBeNil)
				response, err := db.Query(nil, query, "ms", now)
				So(err, ShouldBeNil)
				So(response, ShouldResemble, newResponse(
					1400, 24,
					1600, 26,
					1800, 28,
					2000, 29,
				))
				queryStr, database, epoch := store["delta"].NextQuery()
				So(
					queryStr,
					ShouldEqual,
					"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z'")
				So(database, ShouldEqual, "scotty")
				So(epoch, ShouldEqual, "ms")
				So(store["delta"].NoMoreQueries(), ShouldBeTrue)

				queryStr, database, epoch = store["echo"].NextQuery()
				So(
					queryStr,
					ShouldEqual,
					"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z'")
				So(database, ShouldEqual, "scotty")
				So(epoch, ShouldEqual, "ms")
				So(store["echo"].NoMoreQueries(), ShouldBeTrue)

				queryStr, database, epoch = store["foxtrot"].NextQuery()
				So(
					queryStr,
					ShouldEqual,
					"SELECT mean(value) FROM dual WHERE time >= '2016-11-30T19:01:00Z'")
				So(database, ShouldEqual, "scotty")
				So(epoch, ShouldEqual, "ms")
				So(store["foxtrot"].NoMoreQueries(), ShouldBeTrue)
			})
		})
	})
}
