package com.qubit.metricsflow.metrics.core.event;

import java.io.Serializable;
import java.util.LinkedList;
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

    public MetricUpdateEvent(MetricUpdateEvent that) {
        this.name = that.name;
        this.labelNameValuePairs = new LinkedList<>(that.getLabelNameValuePairs());
        this.value = that.value;
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

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{name: ");
        sb.append(name);
        sb.append("; value: ");
        sb.append(value);
        sb.append("; labels: [");

        labelNameValuePairs.forEach(nvp -> {
            sb.append(nvp.getName());
            sb.append("=");
            sb.append(nvp.getValue());
            sb.append(", ");
        });

        sb.replace(sb.length() - 2, sb.length(), "]}");
        return sb.toString();
    }
}
