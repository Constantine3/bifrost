package otel

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	metricscollector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// blackholeListener accepts TCP connections and then never speaks. This is the shape of a
// realistic misconfiguration — a wrong port, or an ingress that terminates the connection
// but routes nowhere — and it is strictly worse than a closed port, which fails fast.
func blackholeListener(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	var conns atomic.Value
	conns.Store([]net.Conn{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				close(done)
				return
			}
			// Hold the connection open without reading or writing.
			existing, _ := conns.Load().([]net.Conn)
			conns.Store(append(existing, c))
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
		if held, ok := conns.Load().([]net.Conn); ok {
			for _, c := range held {
				_ = c.Close()
			}
		}
	}
}

func testTarget(t *testing.T, client OtelClient, timeout time.Duration) *otelTarget {
	t.Helper()
	return &otelTarget{
		serviceName:   "test-svc",
		url:           "test://target",
		traceType:     TraceTypeGenAIExtension,
		client:        client,
		exportTimeout: timeout,
	}
}

// TestInject_GRPCExportHonoursTimeout is the regression for the outage. gRPC exports
// carried no deadline of their own and the caller passes context.Background(), so an
// endpoint that completed a TCP handshake but never replied blocked the flush goroutine
// forever — leaking a goroutine and a full trace snapshot per request.
func TestInject_GRPCExportHonoursTimeout(t *testing.T) {
	addr, stop := blackholeListener(t)
	defer stop()

	logger = bifrost.NewDefaultLogger(schemas.LogLevelError)

	client, err := NewOtelClientGRPC(addr, nil, "", true)
	if err != nil {
		t.Fatalf("NewOtelClientGRPC: %v", err)
	}
	defer client.Close()

	const timeout = 400 * time.Millisecond
	plugin := &OtelPlugin{targets: []*otelTarget{testTarget(t, client, timeout)}}

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- plugin.Inject(context.Background(), &schemas.Trace{TraceID: "trace-grpc-timeout"}) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Inject never returned against a black-holed gRPC endpoint")
	}

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Inject took %v against a black-holed endpoint, expected it bounded near %v", elapsed, timeout)
	}
}

// TestInject_HTTPExportHonoursTimeout covers the same bound on the HTTP path, which
// previously relied on a hardcoded 30s client timeout.
func TestInject_HTTPExportHonoursTimeout(t *testing.T) {
	addr, stop := blackholeListener(t)
	defer stop()

	logger = bifrost.NewDefaultLogger(schemas.LogLevelError)

	const timeout = 400 * time.Millisecond
	client, err := NewOtelClientHTTP("http://"+addr, nil, "", true, timeout)
	if err != nil {
		t.Fatalf("NewOtelClientHTTP: %v", err)
	}
	defer client.Close()

	plugin := &OtelPlugin{targets: []*otelTarget{testTarget(t, client, timeout)}}

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- plugin.Inject(context.Background(), &schemas.Trace{TraceID: "trace-http-timeout"}) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Inject never returned against a black-holed HTTP endpoint")
	}

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Inject took %v against a black-holed endpoint, expected it bounded near %v", elapsed, timeout)
	}
}

// failingClient always errors, standing in for a permanently misconfigured collector.
type failingClient struct{ calls atomic.Int64 }

func (c *failingClient) Emit(_ context.Context, _ []*ResourceSpan) error {
	c.calls.Add(1)
	return errors.New("collector unreachable")
}
func (c *failingClient) Close() error { return nil }

// Exercise the real Init -> Inject -> OTLP metrics path, including collections with
// no intervening requests. The profiles deliberately share their resource identity
// and metrics destination so their counters must carry distinct profile labels.
func TestTraceExportMetrics(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "cumulative")
	var recovered atomic.Bool
	var traceCalls atomic.Int64
	var mu sync.Mutex
	var received []*metricscollector.ExportMetricsServiceRequest
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/failing-traces":
			traceCalls.Add(1)
			if !recovered.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		case "/healthy-traces":
		case "/v1/metrics":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read metrics: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var req metricscollector.ExportMetricsServiceRequest
			if err := proto.Unmarshal(body, &req); err != nil {
				t.Errorf("decode OTLP metrics: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			received = append(received, &req)
			mu.Unlock()
		default:
			t.Errorf("unexpected collector path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(collector.Close)

	profiles := []*Profile{{Enabled: false}}
	for _, path := range []string{"/failing-traces", "/healthy-traces"} {
		profiles = append(profiles, &Profile{
			Enabled: true, TracesEnabled: true, MetricsEnabled: true,
			ServiceName: "same-service", Protocol: ProtocolHTTP, Insecure: true,
			CollectorURL:        schemas.NewSecretVar(collector.URL + path),
			MetricsEndpoint:     schemas.NewSecretVar(collector.URL + "/v1/metrics"),
			MetricsPushInterval: 300,
		})
	}
	metricsOnly := *profiles[2]
	metricsOnly.TracesEnabled = false
	metricsOnly.CollectorURL = nil
	tracesOnly := *profiles[2]
	tracesOnly.MetricsEnabled = false
	tracesOnly.MetricsEndpoint = nil
	profiles = append(profiles, &metricsOnly, &tracesOnly)
	plugin, err := Init(context.Background(), &Config{Profiles: profiles}, testLogger{}, nil, "")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = plugin.Cleanup() })

	check := func(wantFailed, wantSuppressed int64) {
		t.Helper()
		mu.Lock()
		received = nil
		mu.Unlock()
		for _, target := range plugin.targets {
			if target.metricsExporter == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := target.metricsExporter.provider.ForceFlush(ctx)
			cancel()
			if err != nil {
				t.Fatalf("ForceFlush: %v", err)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		seen := make(map[string]map[int64]int64)
		for _, req := range received {
			for _, rm := range req.ResourceMetrics {
				for _, sm := range rm.ScopeMetrics {
					for _, m := range sm.Metrics {
						if m.Name != "bifrost_otel_trace_exports_failed_total" && m.Name != "bifrost_otel_trace_exports_suppressed_total" {
							continue
						}
						sum := m.GetSum()
						if sum == nil || !sum.IsMonotonic || sum.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
							t.Fatalf("%s must be a cumulative monotonic sum, got %v", m.Name, m.Data)
						}
						if len(sum.DataPoints) != 1 {
							t.Fatalf("%s points = %d, want 1 per profile", m.Name, len(sum.DataPoints))
						}
						dp := sum.DataPoints[0]
						index := int64(-1)
						instance := ""
						for _, attr := range dp.Attributes {
							switch attr.Key {
							case "otel_profile_index":
								index = attr.Value.GetIntValue()
							case "bifrost_instance_id":
								instance = attr.Value.GetStringValue()
							default:
								t.Errorf("unexpected export-health label: %s", attr.Key)
							}
						}
						if instance != serviceInstanceID {
							t.Errorf("instance = %q, want %q", instance, serviceInstanceID)
						}
						if seen[m.Name] == nil {
							seen[m.Name] = make(map[int64]int64)
						}
						if _, duplicate := seen[m.Name][index]; duplicate {
							t.Errorf("duplicate %s for profile %d", m.Name, index)
						}
						seen[m.Name][index] = dp.GetAsInt()
					}
				}
			}
		}
		for name, want := range map[string]int64{
			"bifrost_otel_trace_exports_failed_total":     wantFailed,
			"bifrost_otel_trace_exports_suppressed_total": wantSuppressed,
		} {
			values := seen[name]
			if len(values) != 2 || values[1] != want || values[2] != 0 {
				t.Errorf("%s = %v, want profile 1: %d, profile 2: 0", name, values, want)
			}
		}
	}

	check(0, 0)
	trace := &schemas.Trace{TraceID: "export-health"}
	for range breakerFailureThreshold + 3 {
		if err := plugin.Inject(context.Background(), trace); err != nil {
			t.Fatal(err)
		}
	}
	if got := traceCalls.Load(); got != breakerFailureThreshold {
		t.Fatalf("trace export attempts = %d, want %d", got, breakerFailureThreshold)
	}
	check(breakerFailureThreshold, 3)
	check(breakerFailureThreshold, 3) // Collection must not reset or re-add the totals.

	recovered.Store(true)
	plugin.targets[0].breakerOpenUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if err := plugin.Inject(context.Background(), trace); err != nil {
		t.Fatal(err)
	}
	if plugin.targets[0].breakerOpenUntil.Load() != 0 {
		t.Fatal("successful probe did not close the breaker")
	}
	check(breakerFailureThreshold, 3) // Recovery keeps the lifetime totals.
}

type exportHealthMetricsServer struct {
	metricscollector.UnimplementedMetricsServiceServer
	requests chan *metricscollector.ExportMetricsServiceRequest
}

func (s *exportHealthMetricsServer) Export(ctx context.Context, req *metricscollector.ExportMetricsServiceRequest) (*metricscollector.ExportMetricsServiceResponse, error) {
	select {
	case s.requests <- req:
		return &metricscollector.ExportMetricsServiceResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestTraceExportMetricsGRPC(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "cumulative")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	receiver := &exportHealthMetricsServer{requests: make(chan *metricscollector.ExportMetricsServiceRequest, 4)}
	// Only metrics are accepted: trace exports receive gRPC Unimplemented.
	metricscollector.RegisterMetricsServiceServer(server, receiver)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	plugin, err := Init(context.Background(), &Config{Profiles: []*Profile{{
		Enabled: true, TracesEnabled: true, MetricsEnabled: true,
		Protocol: ProtocolGRPC, Insecure: true,
		CollectorURL:        schemas.NewSecretVar(listener.Addr().String()),
		MetricsEndpoint:     schemas.NewSecretVar(listener.Addr().String()),
		MetricsPushInterval: 300,
	}}}, testLogger{}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plugin.Cleanup() })
	for range breakerFailureThreshold + 3 {
		if err := plugin.Inject(context.Background(), &schemas.Trace{TraceID: "grpc-export-health"}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := plugin.targets[0].metricsExporter.provider.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-receiver.requests:
		values := make(map[string]int64)
		for _, rm := range req.ResourceMetrics {
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					for _, dp := range m.GetSum().GetDataPoints() {
						values[m.Name] += dp.GetAsInt()
					}
				}
			}
		}
		if values["bifrost_otel_trace_exports_failed_total"] != breakerFailureThreshold || values["bifrost_otel_trace_exports_suppressed_total"] != 3 {
			t.Fatalf("gRPC export health = %v, want failed=5, suppressed=3", values)
		}
	case <-ctx.Done():
		t.Fatal("no OTLP metrics received over gRPC")
	}
}

func TestTraceExportMetricsConcurrentCollection(t *testing.T) {
	logger = testLogger{}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	exporter := &MetricsExporter{provider: provider, meter: provider.Meter("test")}
	exporter.initMetrics()
	t.Cleanup(func() { _ = exporter.Shutdown(context.Background()) })
	target := testTarget(t, &failingClient{}, time.Second)
	target.metricsExporter = exporter
	if err := exporter.registerTraceExportMetrics(target, 0); err != nil {
		t.Fatal(err)
	}
	plugin := &OtelPlugin{targets: []*otelTarget{target}}
	trace := &schemas.Trace{TraceID: "concurrent-export-health"}
	const requests = 128
	var wg sync.WaitGroup
	for range requests {
		wg.Go(func() {
			if err := plugin.Inject(context.Background(), trace); err != nil {
				t.Error(err)
			}
		})
	}
	defer wg.Wait()
	var rm metricdata.ResourceMetrics
	for range 10 {
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	values := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					values[m.Name] += dp.Value
				}
			}
		}
	}
	failed, suppressed := values["bifrost_otel_trace_exports_failed_total"], values["bifrost_otel_trace_exports_suppressed_total"]
	if failed != target.failedExports.Load() || suppressed != target.suppressedExports.Load() || failed+suppressed != requests {
		t.Fatalf("concurrent totals = %v, want every one of %d requests counted exactly once", values, requests)
	}
}

// Initialization and final metrics export are outside the timed loop. Trace export
// is stubbed to isolate Inject from network I/O, with an empty trace in each case.
func BenchmarkInjectExportHealth(b *testing.B) {
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	b.Cleanup(collector.Close)
	for _, tc := range []struct {
		name           string
		metricsEnabled bool
		breakerOpen    bool
	}{
		{"metrics_disabled/healthy", false, false},
		{"metrics_disabled/breaker_open", false, true},
		{"metrics_enabled/healthy", true, false},
		{"metrics_enabled/breaker_open", true, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			plugin, err := Init(context.Background(), &Config{Profiles: []*Profile{{
				Enabled: true, TracesEnabled: true, MetricsEnabled: tc.metricsEnabled,
				Protocol: ProtocolHTTP, Insecure: true,
				CollectorURL:        schemas.NewSecretVar(collector.URL + "/v1/traces"),
				MetricsEndpoint:     schemas.NewSecretVar(collector.URL + "/v1/metrics"),
				MetricsPushInterval: 300,
			}}}, testLogger{}, nil, "")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = plugin.Cleanup() })
			target := plugin.targets[0]
			_ = target.client.Close()
			target.client = stubOtelClient{}
			if tc.breakerOpen {
				target.breakerOpenUntil.Store(time.Now().Add(time.Hour).UnixNano())
			}
			trace := &schemas.Trace{TraceID: "bench"}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = plugin.Inject(context.Background(), trace)
			}
		})
	}
}

// TestInject_CircuitBreakerStopsDialling verifies that a permanently bad endpoint stops
// costing an export attempt per request. Without the breaker, every request pays the full
// export timeout for as long as the misconfiguration lasts.
func TestInject_CircuitBreakerStopsDialling(t *testing.T) {
	logger = bifrost.NewDefaultLogger(schemas.LogLevelError)

	client := &failingClient{}
	target := testTarget(t, client, time.Second)
	plugin := &OtelPlugin{targets: []*otelTarget{target}}

	// Drive well past the threshold.
	const attempts = breakerFailureThreshold + 50
	for i := range attempts {
		if err := plugin.Inject(context.Background(), &schemas.Trace{TraceID: "trace-breaker"}); err != nil {
			t.Fatalf("Inject returned an error on attempt %d: %v", i, err)
		}
	}

	if calls := client.calls.Load(); calls > breakerFailureThreshold {
		t.Fatalf("breaker did not open: %d export attempts for %d injects (threshold %d)", calls, attempts, breakerFailureThreshold)
	}
	if suppressed := target.suppressedExports.Load(); suppressed == 0 {
		t.Fatal("expected suppressed exports once the breaker opened, got 0")
	}
}

// TestBreakerRecoversAfterCooldown verifies the breaker lets a probe through once the
// cooldown elapses, so a collector that comes back is picked up again.
func TestBreakerRecoversAfterCooldown(t *testing.T) {
	target := &otelTarget{url: "test://recover"}

	for range breakerFailureThreshold {
		target.tripBreaker()
	}
	if !target.breakerOpen() {
		t.Fatal("breaker should be open after reaching the failure threshold")
	}

	// Expire the cooldown.
	target.breakerOpenUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())
	if target.breakerOpen() {
		t.Fatal("breaker should allow a probe once the cooldown has elapsed")
	}

	target.resetBreaker()
	if target.breakerOpen() {
		t.Fatal("breaker should stay closed after a successful export")
	}
	if got := target.consecutiveFailures.Load(); got != 0 {
		t.Fatalf("consecutiveFailures = %d after reset, want 0", got)
	}
}

// TestBreakerAllowsSingleProbeUnderConcurrency locks in the half-open guarantee. A batch of
// traces flushing at the instant the cooldown expires races on the same transition, and only
// one of them may dial a collector that is probably still dead. If every racer got through,
// each would block for a full exportTimeout, which is the cost the breaker exists to avoid.
func TestBreakerAllowsSingleProbeUnderConcurrency(t *testing.T) {
	target := &otelTarget{url: "test://concurrent-probe"}

	for range breakerFailureThreshold {
		target.tripBreaker()
	}
	// Expire the cooldown so every goroutine below sees the breaker as probe-ready.
	target.breakerOpenUntil.Store(time.Now().Add(-time.Millisecond).UnixNano())

	const callers = 64
	var allowed atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			<-start
			if !target.breakerOpen() {
				allowed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := allowed.Load(); got != 1 {
		t.Fatalf("breaker allowed %d probes through, want exactly 1", got)
	}
	if got := target.suppressedExports.Load(); got != callers-1 {
		t.Fatalf("suppressedExports = %d, want %d", got, callers-1)
	}
}

func TestResolveExportTimeout(t *testing.T) {
	cases := []struct {
		name    string
		seconds int
		want    time.Duration
		wantErr bool
	}{
		{name: "unset uses default", seconds: 0, want: DefaultExportTimeout},
		{name: "explicit value", seconds: 12, want: 12 * time.Second},
		{name: "at max", seconds: int(MaxExportTimeout / time.Second), want: MaxExportTimeout},
		{name: "negative rejected", seconds: -1, wantErr: true},
		{name: "above max rejected", seconds: int(MaxExportTimeout/time.Second) + 1, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveExportTimeout(tc.seconds)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %d seconds", tc.seconds)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveExportTimeout(%d) = %v, want %v", tc.seconds, got, tc.want)
			}
		})
	}
}
