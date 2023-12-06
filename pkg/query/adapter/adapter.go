// Copyright 2023 EasyStack, Inc.
// Licensed under the Apache License 2.0.

package adapter

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/api"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	DedupParam               = "dedup"
	PartialResponseParam     = "partial_response"
	MaxSourceResolutionParam = "max_source_resolution"
	ReplicaLabelsParam       = "replicaLabels[]"
	StoreMatcherParam        = "storeMatch[]"
	ShardInfoParam           = "shard_info"
	MatcherParam             = "match[]"

	defaultLookbackDelta = 5 * time.Minute
)

// PrometheusAdapter is a sneaky component that is able to export Thanos data directly via Prometheus native federate API.
// Experimental and not recommended: Done only for compatibility and migration purposes.
// Why not recommended?
// * Federate endpoint is not recommended as it tries to replicate metric database with requirement of highly available network (double scrape).
// Also all warnings (e.g related to partial responses) are only logged, as federate API has no way of handling those.
type PrometheusAdapter struct {
	logger          log.Logger
	queryableCreate query.QueryableCreator

	enableAutodownsampling     bool
	enableQueryPartialResponse bool
	enableQueryPushdown        bool

	replicaLabels                          []string
	defaultInstantQueryMaxSourceResolution time.Duration
}

func NewPrometheus(
	logger log.Logger,
	c query.QueryableCreator,
	enableAutodownsampling bool,
	enableQueryPartialResponse bool,
	enableQueryPushdown bool,
	replicaLabels []string,
	defaultInstantQueryMaxSourceResolution time.Duration,
) *PrometheusAdapter {
	return &PrometheusAdapter{
		logger:                                 logger,
		queryableCreate:                        c,
		enableAutodownsampling:                 enableAutodownsampling,
		enableQueryPartialResponse:             enableQueryPartialResponse,
		enableQueryPushdown:                    enableQueryPushdown,
		replicaLabels:                          replicaLabels,
		defaultInstantQueryMaxSourceResolution: defaultInstantQueryMaxSourceResolution,
	}
}

// Register the API's endpoints in the given router.
func (ad *PrometheusAdapter) Register(r *route.Router, tracer opentracing.Tracer, ins extpromhttp.InstrumentationMiddleware) {
	r.Get("/federate", ins.NewHandler("federate", tracing.HTTPMiddleware(tracer, "federate", ad.logger, gziphandler.GzipHandler(http.HandlerFunc(ad.federation)))))
}

func (ad *PrometheusAdapter) federation(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("error parsing form values: %v", err), http.StatusBadRequest)
		return
	}

	var matcherSets [][]*labels.Matcher
	for _, s := range r.Form[MatcherParam] {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		matcherSets = append(matcherSets, matchers)
	}

	enableDedup, apiErr := ad.parseEnableDedupParam(r)
	if apiErr != nil {
		http.Error(w, apiErr.Error(), http.StatusBadRequest)
		return
	}

	replicaLabels, apiErr := ad.parseReplicaLabelsParam(r)
	if apiErr != nil {
		http.Error(w, apiErr.Error(), http.StatusBadRequest)
		return
	}

	storeDebugMatchers, apiErr := ad.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		http.Error(w, apiErr.Error(), http.StatusBadRequest)
		return
	}

	maxSourceResolution, apiErr := ad.parseDownsamplingParamMillis(r, ad.defaultInstantQueryMaxSourceResolution)
	if apiErr != nil {
		http.Error(w, apiErr.Error(), http.StatusBadRequest)
		return
	}

	shardInfo, apiErr := ad.parseShardInfo(r)
	if apiErr != nil {
		http.Error(w, apiErr.Error(), http.StatusBadRequest)
		return
	}

	enablePartialResponse, apiErr := ad.parsePartialResponseParam(r, ad.enableQueryPartialResponse)
	if apiErr != nil {
		http.Error(w, apiErr.Error(), http.StatusBadRequest)
		return
	}

	var (
		mint   = timestamp.FromTime(time.Now().Add(-defaultLookbackDelta))
		maxt   = timestamp.FromTime(time.Now())
		format = expfmt.Negotiate(r.Header)
		enc    = expfmt.NewEncoder(w, format)
	)
	w.Header().Set("Content-Type", string(format))

	q, err := ad.queryableCreate(
		enableDedup,
		replicaLabels,
		storeDebugMatchers,
		maxSourceResolution,
		enablePartialResponse,
		ad.enableQueryPushdown,
		false,
		shardInfo,
		query.NoopSeriesStatsReporter,
	).Querier(r.Context(), mint, maxt)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer runutil.CloseWithLogOnErr(ad.logger, q, "queryable series")

	vec := make(promql.Vector, 0, 8000)
	hints := &storage.SelectHints{Start: mint, End: maxt}

	var sets []storage.SeriesSet
	for _, mset := range matcherSets {
		qs := q.Select(true, hints, mset...)
		if qs.Warnings() != nil {
			_ = level.Warn(ad.logger).Log("msg", "federation select returned warnings", "warnings", qs.Warnings())
		}
		if qs.Err() != nil {
			http.Error(w, qs.Err().Error(), http.StatusInternalServerError)
			return
		}
		sets = append(sets, qs)
	}

	var chkIter chunkenc.Iterator
	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	it := storage.NewBuffer(int64(defaultLookbackDelta / 1e6))

Loop:
	for set.Next() {
		s := set.At()
		it.Reset(s.Iterator(chkIter))

		var (
			t  int64
			v  float64
			h  *histogram.Histogram
			fh *histogram.FloatHistogram
			ok bool
		)
		valueType := it.Seek(maxt)
		switch valueType {
		case chunkenc.ValFloat:
			t, v = it.At()
		case chunkenc.ValFloatHistogram, chunkenc.ValHistogram:
			t, fh = it.AtFloatHistogram()
		default:
			t, v, h, fh, ok = it.PeekBack(1)
			if !ok {
				continue Loop
			}
			if h != nil {
				fh = h.ToFloat()
			}
		}
		// The exposition formats do not support stale markers, so drop them. This
		// is good enough for staleness handling of federated data, as the
		// interval-based limits on staleness will do the right thing for supported
		// use cases (which is to say federating aggregated time series).
		if value.IsStaleNaN(v) {
			continue
		}

		vec = append(vec, promql.Sample{
			Metric: s.Labels(),
			Point:  promql.Point{T: t, V: v, H: fh},
		})
	}
	if set.Err() != nil {
		http.Error(w, set.Err().Error(), http.StatusInternalServerError)
		return
	}

	sort.Sort(byName(vec))

	var (
		lastMetricName                          string
		lastWasHistogram, lastHistogramWasGauge bool
		protMetricFam                           *dto.MetricFamily
	)
	for _, s := range vec {
		isHistogram := s.H != nil
		if isHistogram &&
			format != expfmt.FmtProtoDelim && format != expfmt.FmtProtoText && format != expfmt.FmtProtoCompact {
			// Can't serve the native histogram.
			// TODO(codesome): Serve them when other protocols get the native histogram support.
			continue
		}

		nameSeen := false
		protMetric := &dto.Metric{}

		err := s.Metric.Validate(func(l labels.Label) error {
			if l.Value == "" {
				// No value means unset. Never consider those labels.
				// This is also important to protect against nameless metrics.
				return nil
			}
			if l.Name == labels.MetricName {
				nameSeen = true
				if l.Value == lastMetricName && // We already have the name in the current MetricFamily, and we ignore nameless metrics.
					lastWasHistogram == isHistogram && // The sample type matches (float vs histogram).
					// If it was a histogram, the histogram type (counter vs gauge) also matches.
					(!isHistogram || lastHistogramWasGauge == (s.H.CounterResetHint == histogram.GaugeType)) {
					return nil
				}

				// Since we now check for the sample type and type of histogram above, we will end up
				// creating multiple metric families for the same metric name. This would technically be
				// an invalid exposition. But since the consumer of this is Prometheus, and Prometheus can
				// parse it fine, we allow it and bend the rules to make federation possible in those cases.

				// Need to start a new MetricFamily. Ship off the old one (if any) before
				// creating the new one.
				if protMetricFam != nil {
					if err := enc.Encode(protMetricFam); err != nil {
						return err
					}
				}
				protMetricFam = &dto.MetricFamily{
					Type: dto.MetricType_UNTYPED.Enum(),
					Name: proto.String(l.Value),
				}
				if isHistogram {
					if s.H.CounterResetHint == histogram.GaugeType {
						protMetricFam.Type = dto.MetricType_GAUGE_HISTOGRAM.Enum()
					} else {
						protMetricFam.Type = dto.MetricType_HISTOGRAM.Enum()
					}
				}
				lastMetricName = l.Value
				return nil
			}
			protMetric.Label = append(protMetric.Label, &dto.LabelPair{
				Name:  proto.String(l.Name),
				Value: proto.String(l.Value),
			})
			return nil
		})
		if err != nil {
			_ = level.Error(ad.logger).Log("msg", "federation failed", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !nameSeen {
			_ = level.Warn(ad.logger).Log("msg", "Ignoring nameless metric during federation", "metric", s.Metric)
			continue
		}

		protMetric.TimestampMs = proto.Int64(s.T)
		if !isHistogram {
			lastHistogramWasGauge = false
			protMetric.Untyped = &dto.Untyped{
				Value: proto.Float64(s.V),
			}
		} else {
			lastHistogramWasGauge = s.H.CounterResetHint == histogram.GaugeType
			protMetric.Histogram = &dto.Histogram{
				SampleCountFloat: proto.Float64(s.H.Count),
				SampleSum:        proto.Float64(s.H.Sum),
				Schema:           proto.Int32(s.H.Schema),
				ZeroThreshold:    proto.Float64(s.H.ZeroThreshold),
				ZeroCountFloat:   proto.Float64(s.H.ZeroCount),
				NegativeCount:    s.H.NegativeBuckets,
				PositiveCount:    s.H.PositiveBuckets,
			}
			if len(s.H.PositiveSpans) > 0 {
				protMetric.Histogram.PositiveSpan = make([]*dto.BucketSpan, len(s.H.PositiveSpans))
				for i, sp := range s.H.PositiveSpans {
					protMetric.Histogram.PositiveSpan[i] = &dto.BucketSpan{
						Offset: proto.Int32(sp.Offset),
						Length: proto.Uint32(sp.Length),
					}
				}
			}
			if len(s.H.NegativeSpans) > 0 {
				protMetric.Histogram.NegativeSpan = make([]*dto.BucketSpan, len(s.H.NegativeSpans))
				for i, sp := range s.H.NegativeSpans {
					protMetric.Histogram.NegativeSpan[i] = &dto.BucketSpan{
						Offset: proto.Int32(sp.Offset),
						Length: proto.Uint32(sp.Length),
					}
				}
			}
		}
		lastWasHistogram = isHistogram
		protMetricFam.Metric = append(protMetricFam.Metric, protMetric)
	}

	// Still have to ship off the last MetricFamily, if any.
	if protMetricFam != nil {
		if err := enc.Encode(protMetricFam); err != nil {
			_ = level.Error(ad.logger).Log("msg", "federation failed", "err", err)
		}
	}
}

func (ad *PrometheusAdapter) parseEnableDedupParam(r *http.Request) (enableDeduplication bool, _ *api.ApiError) {
	enableDeduplication = true

	if val := r.FormValue(DedupParam); val != "" {
		var err error
		enableDeduplication, err = strconv.ParseBool(val)
		if err != nil {
			return false, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", DedupParam)}
		}
	}
	return enableDeduplication, nil
}

func (ad *PrometheusAdapter) parseReplicaLabelsParam(r *http.Request) (replicaLabels []string, _ *api.ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	replicaLabels = ad.replicaLabels
	// Overwrite the cli flag when provided as a query parameter.
	if len(r.Form[ReplicaLabelsParam]) > 0 {
		replicaLabels = r.Form[ReplicaLabelsParam]
	}

	return replicaLabels, nil
}

func (ad *PrometheusAdapter) parseStoreDebugMatchersParam(r *http.Request) (storeMatchers [][]*labels.Matcher, _ *api.ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	for _, s := range r.Form[StoreMatcherParam] {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
		}
		storeMatchers = append(storeMatchers, matchers)
	}

	return storeMatchers, nil
}

func (ad *PrometheusAdapter) parseDownsamplingParamMillis(r *http.Request, defaultVal time.Duration) (maxResolutionMillis int64, _ *api.ApiError) {
	maxSourceResolution := 0 * time.Second

	val := r.FormValue(MaxSourceResolutionParam)
	if ad.enableAutodownsampling || (val == "auto") {
		maxSourceResolution = defaultVal
	}
	if val != "" && val != "auto" {
		var err error
		maxSourceResolution, err = parseDuration(val)
		if err != nil {
			return 0, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", MaxSourceResolutionParam)}
		}
	}

	if maxSourceResolution < 0 {
		return 0, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("negative '%s' is not accepted. Try a positive integer", MaxSourceResolutionParam)}
	}

	return int64(maxSourceResolution / time.Millisecond), nil
}

func (ad *PrometheusAdapter) parsePartialResponseParam(r *http.Request, defaultEnablePartialResponse bool) (enablePartialResponse bool, _ *api.ApiError) {
	// Overwrite the cli flag when provided as a query parameter.
	if val := r.FormValue(PartialResponseParam); val != "" {
		var err error
		defaultEnablePartialResponse, err = strconv.ParseBool(val)
		if err != nil {
			return false, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", PartialResponseParam)}
		}
	}
	return defaultEnablePartialResponse, nil
}

func (ad *PrometheusAdapter) parseShardInfo(r *http.Request) (*storepb.ShardInfo, *api.ApiError) {
	data := r.FormValue(ShardInfoParam)
	if data == "" {
		return nil, nil
	}

	if len(data) == 0 {
		return nil, nil
	}

	var info storepb.ShardInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "could not unmarshal parameter %s", ShardInfoParam)}
	}

	return &info, nil
}

// byName makes a model.Vector sortable by metric name.
type byName promql.Vector

func (vec byName) Len() int      { return len(vec) }
func (vec byName) Swap(i, j int) { vec[i], vec[j] = vec[j], vec[i] }

func (vec byName) Less(i, j int) bool {
	ni := vec[i].Metric.Get(labels.MetricName)
	nj := vec[j].Metric.Get(labels.MetricName)
	return ni < nj
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
}
