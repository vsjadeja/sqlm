package sqlm

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/vsjadeja/metric"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	_ prometheus.Collector = new(QMonitor)

	defaultQMonitor = NewQMonitor()

	// this regexp is used to clean-up sequences of ?,?,? in metrics dump and convert them in one ?
	queryCleanup = regexp.MustCompile(`(,\?)+`)
	// replace (?), blocks in insert query to empty line
	insertQueryCleanup = regexp.MustCompile(`(?i)values \(.*$`)

	defaultTracer = otel.Tracer("storage")
)

type Key string

const (
	SqlType       = `sqlType`
	SqlMaster     = `master`
	SqlSlave      = `slave`
	Begin     Key = `begin`
	QuerySpan Key = `querySpan`
)

type QHooks struct {
	RWHost     string //Master Database Host
	RWDatabase string //Master Database Name
	RWUser     string //Master Database User
	ROHost     string //Slave Database Host
	RODatabase string //Slave Database Name
	ROUser     string //Slave Database User
}

type QErrorHook struct{}

func (h *QHooks) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	var spanName = ""
	var isSelectQuery bool = false
	cleanQuery := queryCleanup.ReplaceAllString(query, "")
	switch {
	case strings.HasPrefix(strings.ToLower(query), "select"):
		spanName = "SQL: SELECT"
		isSelectQuery = true
	case strings.HasPrefix(strings.ToLower(query), "update"):
		spanName = "SQL: UPDATE"
	case strings.HasPrefix(strings.ToLower(query), "delete"):
		spanName = "SQL: DELETE"
	case strings.HasPrefix(strings.ToLower(query), "create"):
		spanName = "SQL: CREATE"
	default:
		spanName = "SQL: OTHER"
		cleanQuery = insertQueryCleanup.ReplaceAllString(query, "VALUES (? ?)")
	}

	ctx, span := defaultTracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(
		attribute.String("service.name", "mysql"),
		attribute.String("db.host", h.getDBHostName(isSelectQuery)),
		attribute.String("db.database", h.getDatabaseName(isSelectQuery)),
		attribute.String("db.user", h.getDBUserName(isSelectQuery)),
		attribute.String("query", cleanQuery),
	)

	return context.WithValue(context.WithValue(ctx, Begin, time.Now()), QuerySpan, span), nil
}

func (h *QHooks) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	query = queryCleanup.ReplaceAllString(query, "")

	sqlType := SqlMaster
	if ctxSqlType := ctx.Value(SqlType); ctxSqlType != nil {
		sqlType = ctxSqlType.(string)
	}

	if strings.HasPrefix(strings.ToLower(query), "insert") {
		query = insertQueryCleanup.ReplaceAllString(query, "VALUES (? ?)")
	}

	begin := ctx.Value(Begin).(time.Time)
	if querySpan := ctx.Value(QuerySpan); querySpan != nil {
		span := querySpan.(trace.Span)
		span.SetAttributes(attribute.String("query.time", fmt.Sprintf("%v", time.Since(begin))))
		span.End()
	}

	defaultQMonitor.StoreTotal(query, sqlType)
	defaultQMonitor.StoreSuccesful(query, sqlType)
	defaultQMonitor.StoreLatency(query, time.Since(begin), sqlType)
	return ctx, nil
}

func (h *QHooks) OnError(ctx context.Context, err error, query string, args ...interface{}) error {
	query = queryCleanup.ReplaceAllString(query, "")
	if err == driver.ErrSkip || err == nil {
		return nil
	}

	sqlType := SqlMaster
	if ctxSqlType := ctx.Value(sqlType); ctxSqlType != nil {
		sqlType = ctxSqlType.(string)
	}

	defaultQMonitor.StoreTotal(query, sqlType)
	defaultQMonitor.StoreErroneous(query, sqlType)
	begin := ctx.Value(Begin).(time.Time)
	defaultQMonitor.StoreLatency(query, time.Since(begin), sqlType)

	if querySpan := ctx.Value(QuerySpan); querySpan != nil {
		span := querySpan.(trace.Span)
		span.SetAttributes(
			attribute.Bool("error", true),
			attribute.String("errorText", err.Error()),
		)
		span.End()
	}

	return err
}
func (h *QHooks) getDBHostName(isSelectQuery bool) (host string) {
	host = h.RWHost
	if isSelectQuery {
		host = h.ROHost
	}
	return host
}

func (h *QHooks) getDatabaseName(isSelectQuery bool) (name string) {
	name = h.RWDatabase
	if isSelectQuery {
		name = h.RODatabase
	}
	return name
}

func (h *QHooks) getDBUserName(isSelectQuery bool) (user string) {
	user = h.RWUser
	if isSelectQuery {
		user = h.ROUser
	}
	return user
}
func init() {
	prometheus.MustRegister(defaultQMonitor)
}

func DefaultQMonitor() *QMonitor {
	return defaultQMonitor
}

type QMonitor struct {
	total   *prometheus.CounterVec
	success *prometheus.CounterVec
	errors  *prometheus.CounterVec
	latency *prometheus.HistogramVec
}

func NewQMonitor() *QMonitor {
	labels := []string{`query`, `type`}

	return &QMonitor{
		total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metric.Namespace,
			Subsystem: metricSubsystem,
			Name:      "query_total",
			Help:      "The total number of query executions.",
		}, labels),
		success: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metric.Namespace,
			Subsystem: metricSubsystem,
			Name:      "query_success",
			Help:      "The number of successfull query executions.",
		}, labels),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metric.Namespace,
			Subsystem: metricSubsystem,
			Name:      "query_error",
			Help:      "The number of erroneous query executions.",
		}, labels),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metric.Namespace,
			Subsystem: metricSubsystem,
			Name:      "query_latency",
			Help:      "The latency of query execution.",
		}, labels),
	}
}

// Now it removes only the list if IDs.
// Should be updated if necessary
func SanitizeQuery(query string) string {
	var res string
	re := regexp.MustCompile(`(\d(,?))+`)
	res = re.ReplaceAllString(query, "")
	return res
}

func (mon *QMonitor) StoreTotal(name string, sqlType string) {
	mon.total.WithLabelValues(SanitizeQuery(name), sqlType).Inc()
}

func (mon *QMonitor) StoreSuccesful(name string, sqlType string) {
	mon.success.WithLabelValues(SanitizeQuery(name), sqlType).Inc()
}

func (mon *QMonitor) StoreErroneous(name string, sqlType string) {
	mon.errors.WithLabelValues(SanitizeQuery(name), sqlType).Inc()
}

func (mon *QMonitor) StoreLatency(name string, d time.Duration, sqlType string) {
	mon.latency.WithLabelValues(SanitizeQuery(name), sqlType).Observe(d.Seconds())
}

func (mon *QMonitor) Describe(ch chan<- *prometheus.Desc) {
	mon.total.Describe(ch)
	mon.success.Describe(ch)
	mon.errors.Describe(ch)
	mon.latency.Describe(ch)
}

func (mon *QMonitor) Collect(ch chan<- prometheus.Metric) {
	mon.total.Collect(ch)
	mon.success.Collect(ch)
	mon.errors.Collect(ch)
	mon.latency.Collect(ch)
}

const (
	metricSubsystem = `sqlm`
)
