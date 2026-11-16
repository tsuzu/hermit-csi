package driver

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/tsuzu/hermit-csi/pkg/lvm"
	"github.com/tsuzu/hermit-csi/pkg/store"
)

const metricsNamespace = "hermit"

// Backup triggers and results, used as label values.
const (
	triggerPeriodic = "periodic"
	triggerSnapshot = "snapshot"
	triggerUnstage  = "unstage"

	resultSuccess   = "success"   // a new generation was uploaded
	resultUnchanged = "unchanged" // nothing changed since the last upload
	resultFenced    = "fenced"    // another node owns the volume now
	resultFailure   = "failure"
)

// longBuckets covers operations from milliseconds (an unchanged backup) to an
// hour (a full restore of a large volume).
var longBuckets = []float64{0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}

// metrics holds a process's Prometheus metrics. Each Node or Controller gets
// its own registry, so tests do not share state.
type metrics struct {
	reg *prometheus.Registry

	csiOps *prometheus.CounterVec
	csiDur *prometheus.HistogramVec
	s3Reqs *prometheus.CounterVec
	s3Dur  *prometheus.HistogramVec

	// node
	backups        *prometheus.CounterVec
	backupDur      *prometheus.HistogramVec
	uploadedBytes  prometheus.Counter
	uploadedChunks prometheus.Counter
	changedChunks  prometheus.Counter
	rescans        prometheus.Counter
	stages         *prometheus.CounterVec
	restores       *prometheus.CounterVec
	restoreDur     prometheus.Histogram
	restoredBytes  prometheus.Counter

	// controller
	gcRuns        *prometheus.CounterVec
	gcDeleted     prometheus.Counter
	gcLastSuccess prometheus.Gauge
	gcDur         prometheus.Histogram
}

func newMetrics(mode, version string) *metrics {
	reg := prometheus.NewRegistry()
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	f(collectors.NewGoCollector())
	f(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "build_info", Help: "Always 1; labels identify the running driver.",
		ConstLabels: prometheus.Labels{"version": version, "mode": mode},
	}, func() float64 { return 1 }))

	m := &metrics{reg: reg}
	m.csiOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "csi_operations_total",
		Help: "CSI RPCs handled, by method and gRPC status code.",
	}, []string{"method", "code"})
	m.csiDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "csi_operation_duration_seconds",
		Help: "Duration of CSI RPCs.", Buckets: longBuckets,
	}, []string{"method"})
	m.s3Reqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "s3_requests_total",
		Help: "S3 operations, by operation and result (ok, not_found, precondition_failed, canceled, error).",
	}, []string{"operation", "result"})
	m.s3Dur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "s3_request_duration_seconds",
		Help: "Duration of S3 operations (a list covers all its pages).", Buckets: prometheus.DefBuckets,
	}, []string{"operation"})
	f(m.csiOps)
	f(m.csiDur)
	f(m.s3Reqs)
	f(m.s3Dur)

	switch mode {
	case "node":
		m.backups = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "backups_total",
			Help: "Backup attempts, by trigger (periodic, snapshot, unstage) and result (success, unchanged, fenced, failure).",
		}, []string{"trigger", "result"})
		m.backupDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "backup_duration_seconds",
			Help: "Duration of backup attempts.", Buckets: longBuckets,
		}, []string{"trigger"})
		m.uploadedBytes = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "backup_uploaded_bytes_total", Help: "Chunk bytes uploaded by backups.",
		})
		m.uploadedChunks = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "backup_uploaded_chunks_total",
			Help: "Chunks uploaded by backups (changed chunks not already in S3).",
		})
		m.changedChunks = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "backup_changed_chunks_total", Help: "Chunks whose content changed between backups.",
		})
		m.rescans = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "backup_full_rescans_total",
			Help: "Backups that had no usable base snapshot and rescanned all allocated blocks instead of using thin_delta.",
		})
		m.stages = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "stages_total",
			Help: "Successful NodeStage operations, by data source: local (the local copy was current) or restore (downloaded from S3).",
		}, []string{"source"})
		m.restores = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "restores_total", Help: "Restores from S3, by result (success, failure).",
		}, []string{"result"})
		m.restoreDur = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "restore_duration_seconds", Help: "Duration of successful restores.", Buckets: longBuckets,
		})
		m.restoredBytes = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "restore_bytes_total", Help: "Non-zero bytes written by successful restores.",
		})
		f(m.backups)
		f(m.backupDur)
		f(m.uploadedBytes)
		f(m.uploadedChunks)
		f(m.changedChunks)
		f(m.rescans)
		f(m.stages)
		f(m.restores)
		f(m.restoreDur)
		f(m.restoredBytes)
	case "controller":
		m.gcRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "gc_runs_total", Help: "Chunk GC runs, by result (success, failure).",
		}, []string{"result"})
		m.gcDeleted = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "gc_deleted_chunks_total", Help: "Unreferenced chunks deleted by GC.",
		})
		m.gcLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Name: "gc_last_success_timestamp_seconds",
			Help: "Unix time of the last successful chunk GC (process start time until the first run).",
		})
		m.gcDur = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "gc_duration_seconds", Help: "Duration of chunk GC runs.", Buckets: longBuckets,
		})
		m.gcLastSuccess.SetToCurrentTime()
		f(m.gcRuns)
		f(m.gcDeleted)
		f(m.gcLastSuccess)
		f(m.gcDur)
	}
	return m
}

// Handler serves the registry in the Prometheus exposition format.
func (m *metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// interceptor counts CSI RPCs and logs them.
func (m *metrics) interceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := logInterceptor(ctx, req, info, h)
	m.csiOps.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
	m.csiDur.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
	return resp, err
}

func (m *metrics) observeBackup(trigger, result string, d time.Duration) {
	m.backups.WithLabelValues(trigger, result).Inc()
	m.backupDur.WithLabelValues(trigger).Observe(d.Seconds())
}

// serveMetrics serves /metrics and /healthz on addr until ctx is done.
func serveMetrics(ctx context.Context, addr string, m *metrics) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	klog.InfoS("metrics listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		klog.ErrorS(err, "metrics server stopped")
	}
}

// ---------------------------------------------------------------- S3

// instrumentedStore counts and times every S3 operation.
type instrumentedStore struct {
	s store.ObjectStore
	m *metrics
}

func instrumentStore(s store.ObjectStore, m *metrics) store.ObjectStore {
	if s == nil {
		return nil
	}
	return instrumentedStore{s: s, m: m}
}

func s3Result(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, store.ErrNotFound):
		return "not_found"
	case errors.Is(err, store.ErrPrecondition):
		return "precondition_failed"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	return "error"
}

func (i instrumentedStore) observe(op string, start time.Time, err error) {
	i.m.s3Reqs.WithLabelValues(op, s3Result(err)).Inc()
	i.m.s3Dur.WithLabelValues(op).Observe(time.Since(start).Seconds())
}

func (i instrumentedStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	start := time.Now()
	b, etag, err := i.s.Get(ctx, key)
	i.observe("get", start, err)
	return b, etag, err
}

func (i instrumentedStore) Put(ctx context.Context, key string, data []byte, o store.PutOpts) (string, error) {
	start := time.Now()
	etag, err := i.s.Put(ctx, key, data, o)
	i.observe("put", start, err)
	return etag, err
}

func (i instrumentedStore) Head(ctx context.Context, key string) (store.ObjectInfo, error) {
	start := time.Now()
	info, err := i.s.Head(ctx, key)
	i.observe("head", start, err)
	return info, err
}

func (i instrumentedStore) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := i.s.Delete(ctx, key)
	i.observe("delete", start, err)
	return err
}

func (i instrumentedStore) List(ctx context.Context, prefix string, fn func(store.ObjectInfo) error) error {
	start := time.Now()
	var fnErr error
	err := i.s.List(ctx, prefix, func(o store.ObjectInfo) error {
		fnErr = fn(o)
		return fnErr
	})
	if fnErr != nil && errors.Is(err, fnErr) {
		i.observe("list", start, nil) // the caller stopped the listing, S3 was fine
	} else {
		i.observe("list", start, err)
	}
	return err
}

// ---------------------------------------------------------------- node state

// poolUsager is implemented by VGs that can report thin pool usage.
type poolUsager interface {
	PoolUsage(ctx context.Context) (lvm.PoolUsage, error)
}

var (
	descLastBackup = prometheus.NewDesc(metricsNamespace+"_volume_last_backup_timestamp_seconds",
		"Unix time of the newest volume state known to be in S3: when the snapshot of the last successful backup was taken, or when the volume was staged.",
		[]string{"volume"}, nil)
	descInterval = prometheus.NewDesc(metricsNamespace+"_volume_backup_interval_seconds",
		"Effective periodic backup interval of a staged volume (0 = periodic backups disabled).",
		[]string{"volume"}, nil)
	descFailures = prometheus.NewDesc(metricsNamespace+"_volume_backup_consecutive_failures",
		"Backup attempts that failed since the last successful one.",
		[]string{"volume"}, nil)
	descGeneration = prometheus.NewDesc(metricsNamespace+"_volume_generation",
		"Generation of the volume's latest backup in S3.",
		[]string{"volume"}, nil)
	descFenced = prometheus.NewDesc(metricsNamespace+"_volume_fenced",
		"1 if another node took over the volume while it is still staged here: this node no longer backs it up.",
		[]string{"volume"}, nil)
	descPoolData = prometheus.NewDesc(metricsNamespace+"_thin_pool_data_usage_ratio",
		"Fraction of the thin pool's data space in use.", []string{"vg", "pool"}, nil)
	descPoolMeta = prometheus.NewDesc(metricsNamespace+"_thin_pool_metadata_usage_ratio",
		"Fraction of the thin pool's metadata space in use.", []string{"vg", "pool"}, nil)
	descPoolSize = prometheus.NewDesc(metricsNamespace+"_thin_pool_size_bytes",
		"Size of the thin pool's data space.", []string{"vg", "pool"}, nil)
)

// nodeCollector reports per-volume backup state from the state files on each
// scrape, so series appear on stage and disappear on unstage without
// bookkeeping, and survive restarts. It also reports thin pool usage.
type nodeCollector struct{ n *Node }

func (c nodeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descLastBackup, descInterval, descFailures, descGeneration, descFenced, descPoolData, descPoolMeta, descPoolSize} {
		ch <- d
	}
}

func (c nodeCollector) Collect(ch chan<- prometheus.Metric) {
	n := c.n
	vols, err := n.states.list()
	if err != nil {
		klog.ErrorS(err, "metrics: list states")
	}
	for _, vol := range vols {
		st, err := n.states.load(vol) // writes are atomic renames, no lock needed
		if err != nil || st == nil || !st.Staged {
			continue
		}
		g := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, vol)
		}
		if !st.LastBackup.IsZero() {
			g(descLastBackup, float64(st.LastBackup.UnixNano())/1e9)
		}
		g(descInterval, n.intervalFor(st).Seconds())
		g(descFailures, float64(st.BackupFailures))
		g(descGeneration, float64(st.Generation))
		fenced := 0.0
		if st.Fenced {
			fenced = 1
		}
		g(descFenced, fenced)
	}

	if pu, ok := n.vg.(poolUsager); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		u, err := pu.PoolUsage(ctx)
		if err != nil {
			klog.V(2).ErrorS(err, "metrics: thin pool usage")
			return
		}
		labels := []string{u.VG, u.Pool}
		ch <- prometheus.MustNewConstMetric(descPoolData, prometheus.GaugeValue, u.DataRatio, labels...)
		ch <- prometheus.MustNewConstMetric(descPoolMeta, prometheus.GaugeValue, u.MetadataRatio, labels...)
		ch <- prometheus.MustNewConstMetric(descPoolSize, prometheus.GaugeValue, float64(u.Size), labels...)
	}
}
