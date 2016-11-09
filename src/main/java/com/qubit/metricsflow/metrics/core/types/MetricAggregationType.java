package com.qubit.metricsflow.metrics.core.types;

import java.io.Serializable;

public enum MetricAggregationType implements Serializable {
    Sum,
    Min,
    Max,
    Mean;

    public static String toMetricNameSuffix(MetricAggregationType type) {
        return type.toString().toLowerCase();
    }
}
