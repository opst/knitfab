package metrics

import (
	"context"
	"time"

	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"k8s.io/client-go/kubernetes"
)

// EventEmitter is an interface for prometheus event emitters.
type EventEmitter interface {
	// Start starts the emitter.
	//
	// # Args
	//
	// - context.Context: a context for the emitter.
	// If the context is canceled or timed out, the emitter will be stopped.
	//
	// # Returns
	//
	// - error: an error if the emitter is stopped with an error.
	Start(context.Context) error

	// Subscribe registers a listener for given filter.
	//
	// # Args
	//
	// - Filter: a filter for metrics.
	//
	// - func(*io_prometheus_client.Metric) error: a callback function for a metric satisfiled the filter.
	Subscribe(Filter, func(*io_prometheus_client.Metric) error)
}

type k8sEmitter struct {
	nodeName  string
	clientSet *kubernetes.Clientset
	interval  time.Duration
	listeners []struct {
		f        Filter
		callback func(*io_prometheus_client.Metric) error
	}
}

// NewURLEmitter creates a new prometheus event emitter for given endpoint.
//
// # Args
//
// - client: a kubernetes client.
//
// - string: an endpoint for prometheus metrics.
//
// - time.Duration: an interval between requesting for prometheus metrics.
//
// # Returns
//
// - EventEmitter: a new prometheus event emitter.
func FromNode(
	client *kubernetes.Clientset, nodeName string, interval time.Duration,
) EventEmitter {
	return &k8sEmitter{nodeName: nodeName, clientSet: client, interval: interval}
}

func (e *k8sEmitter) Start(ctx context.Context) error {
	restClient := e.clientSet.RESTClient()
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		err := func() error {
			result, err := restClient.Get().
				AbsPath("/api/v1/nodes", e.nodeName, "/proxy/metrics").
				Stream(ctx)
			if err != nil {
				return err
			}
			defer result.Close()

			var p expfmt.TextParser
			// parse response
			mf, err := p.TextToMetricFamilies(result)
			if err != nil {
				return err
			}

			for _, l := range e.listeners {
				if err := l.f(mf, l.callback); err != nil {
					return err
				}
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}
}

func (e *k8sEmitter) Subscribe(filter Filter, callback func(*io_prometheus_client.Metric) error) {
	e.listeners = append(e.listeners, struct {
		f        Filter
		callback func(*io_prometheus_client.Metric) error
	}{f: filter, callback: callback})
}

// Match returns true if given metric matches.
//
// # Args
//
// - map[string]*io_prometheus_client.MetricFamily: a map of metric families.
//
// - func(*io_prometheus_client.Metric) error: a callback function for a metric satisfiled the filter.
//
// # Returns
//
// - *io_prometheus_client.Metric: a metric that matches.
//
// - bool: true if a metric matches.
type Filter func(
	map[string]*io_prometheus_client.MetricFamily,
	func(*io_prometheus_client.Metric) error,
) error

func ForKey(key string, mfilt ...MetricFilter) Filter {
	return func(
		mfs map[string]*io_prometheus_client.MetricFamily,
		callback func(*io_prometheus_client.Metric) error,
	) error {

		mf, ok := mfs[key]
		if !ok {
			return nil
		}
	METLIC:
		for _, m := range mf.Metric {
			for _, f := range mfilt {
				if !f(m) {
					continue METLIC
				}
			}
			if err := callback(m); err != nil {
				return err
			}
		}
		return nil
	}
}

func Anything() Filter {
	return func(
		mfs map[string]*io_prometheus_client.MetricFamily,
		callback func(*io_prometheus_client.Metric) error,
	) error {

		for _, mf := range mfs {
			for _, m := range mf.Metric {
				if err := callback(m); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// MetricFilter is a filter for metrics.
type MetricFilter func(*io_prometheus_client.Metric) bool

// WithLabelAndValue matches a metric having a label with given name and value.
func WithLabelAndValue(name string, value string) MetricFilter {
	return func(m *io_prometheus_client.Metric) bool {
		for _, l := range m.Label {
			if l.GetName() == name && l.GetValue() == value {
				return true
			}
		}
		return false
	}
}

// WithLabel matches a metric having a label with given name.
func WithLabel(name string) MetricFilter {
	return func(m *io_prometheus_client.Metric) bool {
		for _, l := range m.Label {
			if l.GetName() == name {
				return true
			}
		}
		return false
	}
}
