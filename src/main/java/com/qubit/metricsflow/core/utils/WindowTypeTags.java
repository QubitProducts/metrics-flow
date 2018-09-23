package com.qubit.metricsflow.core.utils;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class WindowTypeTags {
    public static final TupleTag<KV<MetricUpdateKey, MetricUpdateValue>> FIXED_IN =
        new TupleTag<KV<MetricUpdateKey, MetricUpdateValue>>() {};
    public static final TupleTag<KV<MetricUpdateKey, MetricUpdateValue>> SLIDING_IN =
        new TupleTag<KV<MetricUpdateKey, MetricUpdateValue>>() {};
    public static final TupleTag<MetricUpdateEvent> FIXED_OUT =
        new TupleTag<MetricUpdateEvent>() {};
    public static final TupleTag<MetricUpdateEvent> SLIDING_OUT =
        new TupleTag<MetricUpdateEvent>() {};
}
