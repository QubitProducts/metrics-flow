package com.qubit.metricsflow.core.types;

import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;
import com.qubit.metricsflow.metrics.core.mdef.MetricDefinition;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nonnull;

@DefaultCoder(AvroCoder.class)
public class MetricUpdateKey implements Serializable {
    private final String metricName;
    private final List<LabelNameValuePair> labelNameValuePairs;

    public MetricUpdateKey() {
        metricName = null;
        labelNameValuePairs = null;
    }

    protected MetricUpdateKey(String metricName, List<LabelNameValuePair> labelNameValuePairs) {
        this.metricName = metricName;
        this.labelNameValuePairs = labelNameValuePairs;
        this.labelNameValuePairs.sort(LabelNameValuePair::compareTo);
    }

    public static MetricUpdateKey of(@Nonnull MetricDefinition<?> mdef,
                                     @Nonnull List<LabelNameValuePair> labelNameValuePairs) {
        return MetricUpdateKey.of(mdef.getName(), labelNameValuePairs);
    }

    public static MetricUpdateKey of(@Nonnull String metricName,
                                     @Nonnull List<LabelNameValuePair> labelNameValuePairs) {
        return new MetricUpdateKey(metricName, labelNameValuePairs);
    }

    public String getMetricName() {
        return metricName;
    }

    public List<LabelNameValuePair> getLabelNameValuePairs() {
        return labelNameValuePairs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MetricUpdateKey that = (MetricUpdateKey)o;
        return this.metricName.equals(that.metricName) && this.labelNameValuePairs.equals(that.labelNameValuePairs);
    }

    @Override
    public int hashCode() {
        int result = metricName != null ? metricName.hashCode() : 0;
        result = 31 * result + (labelNameValuePairs != null ? labelNameValuePairs.hashCode() : 0);
        return result;
    }
}
