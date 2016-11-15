package com.qubit.metricsflow.metrics.core.event;

import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;

import javax.annotation.Nonnull;

public final class InnerMetricUpdateEvent implements Serializable {
    private final String name;
    private final List<LabelNameValuePair> labelNameValuePairs;
    private final EnumSet<MetricAggregationType> fixedWindowAggregations;
    private final EnumSet<MetricAggregationType> slidingWindowAggregations;
    private final double value;

    public InnerMetricUpdateEvent(@Nonnull String metricName,
                                  @Nonnull List<LabelNameValuePair> labelNameValuePairs,
                                  @Nonnull EnumSet<MetricAggregationType> fixedWindowAggregations,
                                  @Nonnull EnumSet<MetricAggregationType> slidingWindowAggregations,
                                  double value) {
        this.name = metricName;
        this.labelNameValuePairs = labelNameValuePairs;
        this.fixedWindowAggregations = fixedWindowAggregations;
        this.slidingWindowAggregations = slidingWindowAggregations;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public List<LabelNameValuePair> getLabelNameValuePairs() {
        return labelNameValuePairs;
    }

    public double getValue() {
        return value;
    }

    public EnumSet<MetricAggregationType> getFixedWindowAggregations() {
        return fixedWindowAggregations;
    }

    public EnumSet<MetricAggregationType> getSlidingWindowAggregations() {
        return slidingWindowAggregations;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof InnerMetricUpdateEvent)) {
            return false;
        }
        if (other == this) {
            return true;
        }

        InnerMetricUpdateEvent that = (InnerMetricUpdateEvent)other;
        return this.name.equals(that.name) &&
               this.labelNameValuePairs.equals(that.labelNameValuePairs) &&
               this.fixedWindowAggregations.equals(that.fixedWindowAggregations) &&
               this.slidingWindowAggregations.equals(that.slidingWindowAggregations) &&
               this.value == that.value;
    }
}
