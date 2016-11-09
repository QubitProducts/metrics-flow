package com.qubit.metricsflow.core.utils;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class WindowTypeTags {
    public static final TupleTag<KV<MetricUpdateKey, Double>> FIXED_IN =
        new TupleTag<KV<MetricUpdateKey, Double>>() {};
    public static final TupleTag<KV<MetricUpdateKey, Double>> SLIDING_IN =
        new TupleTag<KV<MetricUpdateKey, Double>>() {};
    public static final TupleTag<MetricUpdateEvent> FIXED_OUT =
        new TupleTag<MetricUpdateEvent>() {};
    public static final TupleTag<MetricUpdateEvent> SLIDING_OUT =
        new TupleTag<MetricUpdateEvent>() {};
}
