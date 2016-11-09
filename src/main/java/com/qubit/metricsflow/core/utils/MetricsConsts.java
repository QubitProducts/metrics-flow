package com.qubit.metricsflow.core.utils;

import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.values.TupleTag;

public class MetricsConsts {
    public static final TupleTag<MetricUpdateEvent> METRICS_TAG = new TupleTag<MetricUpdateEvent>() {};
    public static final String DEFAULT_STREAM_NAME = "__DEFAULT_STREAM__";
}
