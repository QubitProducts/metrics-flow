package com.qubit.metricsflow.metrics.core.mdef;

import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public abstract class MetricDefinitionBuilderBase<M extends MetricDefinition, B extends MetricDefinitionBuilderBase<M, B>> {
    protected String name;
    protected Set<String> labels = new HashSet<>();
    protected EnumSet<MetricAggregationType> fixedWindowAggregations;
    protected EnumSet<MetricAggregationType> slidingWindowAggregations;

    protected MetricDefinitionBuilderBase() {
        fixedWindowAggregations = EnumSet.noneOf(MetricAggregationType.class);
        slidingWindowAggregations = EnumSet.noneOf(MetricAggregationType.class);
    }

    public B named(String name) {
        this.name = name;
        return (B) this;
    }

    public B labels(String... labels) {
        this.labels.addAll(Arrays.asList(labels));
        return (B) this;
    }

    protected B doEnableFixedWindowAggregation(MetricAggregationType aggregationType) {
        fixedWindowAggregations.add(aggregationType);
        return (B) this;
    }

    protected B doEnableSlidingWindowAggregation(MetricAggregationType aggregationType) {
        slidingWindowAggregations.add(aggregationType);
        return (B) this;
    }

    protected void throwExceptionIfParametersAreInvalid() {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Metric name is not set!");
        }
        if (fixedWindowAggregations.isEmpty() && slidingWindowAggregations.isEmpty()) {
            throw new IllegalArgumentException("No aggregations are used for the metric!");
        }
    }

    public abstract M create();
}
