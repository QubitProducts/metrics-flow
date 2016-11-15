package com.qubit.metricsflow.metrics.core.event;

import java.io.Serializable;
import java.util.List;

public class MetricUpdateEvent implements Serializable {
    private String name;
    private List<LabelNameValuePair> labelNameValuePairs;
    private double value;

    public MetricUpdateEvent(String name,
                             List<LabelNameValuePair> labelNameValuePairs, double value) {
        this.name = name;
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
    public boolean equals(Object o) {
        if (o == null || !(o instanceof MetricUpdateEvent)) {
            return false;
        }
        if (o == this) {
            return true;
        }

        MetricUpdateEvent that = (MetricUpdateEvent) o;
        return this.name.equals(that.name) &&
               this.labelNameValuePairs.equals(that.labelNameValuePairs) &&
               this.value == that.value;
    }
}
