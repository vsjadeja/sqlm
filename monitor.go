package sqlm

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	_ GoDBStatsGetter      = new(sql.DB)
	_ prometheus.Collector = new(godbStatsCollector)
)

type GoDBStatsGetter interface {
	Stats() sql.DBStats
}

type godbStatsCollector struct {
	g GoDBStatsGetter
	m godbStatsMetrics
}

func NewGoDBStatsCollector(dbname string, g GoDBStatsGetter) prometheus.Collector {
	var l prometheus.Labels

	if dbname != `` {
		l = prometheus.Labels{`db_name`: dbname}
	}
	return &godbStatsCollector{
		g: g,
		m: godbStatsMetrics{
			{
				prometheus.NewDesc(
					godbprefix+`max_open_conns`,
					`Maximum number of open connections to the database.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return float64(s.MaxOpenConnections) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					godbprefix+`open_conns`,
					`The number of established connections both in use and idle.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return float64(s.OpenConnections) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					godbprefix+`in_use_conns`,
					`The number of connections currently in use.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return float64(s.InUse) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					godbprefix+`idle_conns`,
					`The number of idle connections.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return float64(s.Idle) },
				prometheus.GaugeValue,
			},
			{
				prometheus.NewDesc(
					godbprefix+`waited_for_total`,
					`The total number of connections waited for.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return float64(s.WaitCount) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					godbprefix+`blocked_seconds`,
					`The total time blocked waiting for a new connection.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return s.WaitDuration.Seconds() },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					godbprefix+`closed_max_idle_total`,
					`The total number of connections closed due to SetMaxIdleConns.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return float64(s.MaxIdleClosed) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					godbprefix+`closed_max_idle_time_total`,
					`The total number of connections closed due to SetConnMaxIdleTime.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return float64(s.MaxIdleTimeClosed) },
				prometheus.CounterValue,
			},
			{
				prometheus.NewDesc(
					godbprefix+`closed_max_lifetime_total`,
					`The total number of connections closed due to SetConnMaxLifetime.`,
					nil, l,
				),
				func(s *sql.DBStats) float64 { return float64(s.MaxLifetimeClosed) },
				prometheus.CounterValue,
			},
		},
	}
}

// Describe returns all descriptions of the collector.
func (c *godbStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.m {
		ch <- m.desc
	}
}

// Collect returns the current state of all metrics of the collector.
func (c *godbStatsCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.g.Stats()

	for _, m := range c.m {
		ch <- prometheus.MustNewConstMetric(m.desc, m.vtyp, m.eval(&s))
	}
}

// dbStatsMetrics provide description, value, and value type for sql.DBStats metrics.
type godbStatsMetrics []struct {
	desc *prometheus.Desc
	eval func(*sql.DBStats) float64
	vtyp prometheus.ValueType
}

const (
	godbprefix = namespace + `_` + metricSubsystem + `_db_`
)
