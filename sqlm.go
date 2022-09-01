package sqlm

import (
	"context"
	"database/sql/driver"
	"regexp"
	"strings"

	"time"

	zmmetric "bitbucket.org/orientswiss/metric"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	_ prometheus.Collector = new(QMonitor)

	defaultQMonitor = NewQMonitor()

	// this regexp is used to clean-up sequences of ?,?,? in metrics dump and convert them in one ?
	queryCleanup = regexp.MustCompile(`(,\\?)+`)
	// replace (?), blocks in insert query to empty line
	insertQueryCleanup = regexp.MustCompile("(?i)values \\(.*$")

	defaultTracer = otel.Tracer("storage")
)

type sqlmConstant string

const (
	SqlType                      = `sqlType`
	SqlMaster                    = `master`
	SqlSlave                     = `slave`
	contextBeginKey sqlmConstant = `begin`
	querySpan       sqlmConstant = `querySpan`
)

type QHooks struct {
	DBName string
	DBHost string
}

type QErrorHook struct{}

func (h *QHooks) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	var spanName = ""
	switch {
	case strings.HasPrefix(strings.ToLower(query), "select"):
		spanName = "SQL: SELECT"
	case strings.HasPrefix(strings.ToLower(query), "update"):
		spanName = "SQL: UPDATE"
	case strings.HasPrefix(strings.ToLower(query), "delete"):
		spanName = "SQL: DELETE"
	case strings.HasPrefix(strings.ToLower(query), "create"):
		spanName = "SQL: CREATE"
	default:
		spanName = "SQL: OTHER"
	}

	ctx, span := defaultTracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(
		attribute.String("service.name", "mysql"),
		attribute.String("query", query),
		attribute.String("db.name", h.DBName),
		attribute.String("db.address", h.DBHost),
	)

	return context.WithValue(context.WithValue(ctx, contextBeginKey, time.Now()), querySpan, span), nil
}

func (h *QHooks) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	begin := ctx.Value("begin").(time.Time)
	query = queryCleanup.ReplaceAllString(query, "")
	sqlType := SqlMaster
	if ctxSqlType := ctx.Value(SqlType); ctxSqlType != nil {
		sqlType = ctxSqlType.(string)
	}

	if strings.HasPrefix(strings.ToLower(query), "insert") {
		query = insertQueryCleanup.ReplaceAllString(query, "VALUES (? ?)")
	}
	if querySpan := ctx.Value("querySpan"); querySpan != nil {
		span := querySpan.(trace.Span)
		span.SetAttributes(
			attribute.String("timeTaken", time.Since(begin).String()),
		)
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
	begin := ctx.Value("begin").(time.Time)
	defaultQMonitor.StoreLatency(query, time.Since(begin), sqlType)

	if querySpan := ctx.Value("querySpan"); querySpan != nil {
		span := querySpan.(trace.Span)
		span.SetAttributes(
			attribute.Bool("error", true),
			attribute.String("errorText", err.Error()),
		)
		span.End()
	}

	return err
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
			Namespace: zmmetric.Namespace,
			Subsystem: metricSubsystem,
			Name:      "query_total",
			Help:      "The total number of query executions.",
		}, labels),
		success: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: zmmetric.Namespace,
			Subsystem: metricSubsystem,
			Name:      "query_success",
			Help:      "The number of successfull query executions.",
		}, labels),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: zmmetric.Namespace,
			Subsystem: metricSubsystem,
			Name:      "query_error",
			Help:      "The number of erroneous query executions.",
		}, labels),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: zmmetric.Namespace,
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