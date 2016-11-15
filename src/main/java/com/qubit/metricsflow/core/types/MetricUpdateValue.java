package com.qubit.metricsflow.core.types;

import com.qubit.metricsflow.metrics.core.mdef.MetricDefinition;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import java.io.Serializable;
import java.util.EnumSet;

import javax.annotation.Nonnull;

public class MetricUpdateValue implements Serializable {
    private final EnumSet<MetricAggregationType> fixedWindowAggregations;
    private final EnumSet<MetricAggregationType> slidingWindowAggregations;
    private final double value;

    protected MetricUpdateValue(EnumSet<MetricAggregationType> fixedWindowAggregations,
                             EnumSet<MetricAggregationType> slidingWindowAggregations,
                             double value) {
        this.fixedWindowAggregations = fixedWindowAggregations;
        this.slidingWindowAggregations = slidingWindowAggregations;
        this.value = value;
    }

    public static MetricUpdateValue of(@Nonnull MetricDefinition<?> mdef, double value) {
        return new MetricUpdateValue(mdef.getFixedWindowAggregations(), mdef.getSlidingWindowAggregations(), value);
    }

    public EnumSet<MetricAggregationType> getFixedWindowAggregations() {
        return fixedWindowAggregations;
    }

    public EnumSet<MetricAggregationType> getSlidingWindowAggregations() {
        return slidingWindowAggregations;
    }

    public double getValue() {
        return value;
    }
}
