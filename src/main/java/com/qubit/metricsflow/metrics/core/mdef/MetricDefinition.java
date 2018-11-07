package com.qubit.metricsflow.metrics.core.mdef;

import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import org.apache.beam.sdk.transforms.DoFn;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nonnull;

public abstract class MetricDefinition<R extends MetricRecorderBase> implements Serializable {
    private final String name;
    private final Set<String> labelNames;
    private final EnumSet<MetricAggregationType> fixedWindowAggregations;
    private final EnumSet<MetricAggregationType> slidingWindowAggregations;

    protected MetricDefinition(@Nonnull String name,
                               @Nonnull EnumSet<MetricAggregationType> fixedWindowAggregations,
                               @Nonnull EnumSet<MetricAggregationType> slidingWindowAggregations,
                               @Nonnull Set<String> labelNames) {
        this.name = name;
        this.labelNames = labelNames;
        this.fixedWindowAggregations = fixedWindowAggregations;
        this.slidingWindowAggregations = slidingWindowAggregations;
    }

    public String getName() {
        return name;
    }

    public Set<String> getLabelNames() {
        return labelNames;
    }

    public EnumSet<MetricAggregationType> getAggregationsForWindowType(MetricWindowType windowType) {
        switch (windowType) {
            case Fixed:
                return getFixedWindowAggregations();
            case Sliding:
                return getSlidingWindowAggregations();
            default:
                throw new IllegalArgumentException("Unknown window type: " + windowType.toString());
        }
    }

    public EnumSet<MetricAggregationType> getFixedWindowAggregations() {
        return fixedWindowAggregations;
    }

    public EnumSet<MetricAggregationType> getSlidingWindowAggregations() {
        return slidingWindowAggregations;
    }

    public abstract R record(DoFn.ProcessContext processContext);
}
