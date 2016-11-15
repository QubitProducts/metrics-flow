package com.qubit.metricsflow.core.utils;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.EnumSet;

public class MetricUtils {
    public static final TupleTag<KV<MetricUpdateKey, MetricUpdateValue>> METRICS_TAG =
        new TupleTag<KV<MetricUpdateKey, MetricUpdateValue>>() {};
    public static final String DEFAULT_STREAM_NAME = "__DEFAULT_STREAM__";

    public static EnumSet<MetricAggregationType> getAggregationsForWindowType(MetricUpdateValue metricUpdateValue,
                                                                              MetricWindowType windowType) {
        switch (windowType) {
            case Fixed:
                return metricUpdateValue.getFixedWindowAggregations();
            case Sliding:
                return metricUpdateValue.getSlidingWindowAggregations();
            default:
                throw new IllegalArgumentException("Unknown window type: " + windowType.toString());
        }
    }

}
