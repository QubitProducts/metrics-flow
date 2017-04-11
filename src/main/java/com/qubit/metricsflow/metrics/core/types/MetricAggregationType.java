package com.qubit.metricsflow.metrics.core.types;

import java.io.Serializable;

public enum MetricAggregationType implements Serializable {
    Sum,
    Min,
    Max,
    Mean;

    public static String toMetricNameSuffix(MetricAggregationType type) {
        switch (type) {
            case Sum:
                return "counter";
            case Min:
                return "min_gauge";
            case Max:
                return "max_gauge";
            case Mean:
                return "mean_gauge";
            default:
                throw new RuntimeException("Unknown metric type " + type.toString());
        }
    }
}
