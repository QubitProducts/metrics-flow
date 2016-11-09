package com.qubit.metricsflow.metrics.core.event;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nonnull;

public final class MetricUpdateEvent implements Serializable {
    private String name;
    private List<LabelNameValuePair> labelNameValuePairs;
    private double value;

    // For AvroCoder
    private MetricUpdateEvent() {
        name = "";
        labelNameValuePairs = new LinkedList<>();
        value = 0.0;
    }

    public MetricUpdateEvent(@Nonnull String metricName,
                             @Nonnull List<LabelNameValuePair> labelNameValuePairs,
                             double value) {
        this.name = metricName;
        this.labelNameValuePairs = labelNameValuePairs;
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

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof MetricUpdateEvent)) {
            return false;
        }
        if (other == this) {
            return true;
        }

        MetricUpdateEvent that = (MetricUpdateEvent)other;
        return this.name.equals(that.name) &&
               this.labelNameValuePairs.equals(that.labelNameValuePairs) &&
               this.value == that.value;
    }
}
