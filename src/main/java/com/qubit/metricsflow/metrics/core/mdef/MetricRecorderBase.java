package com.qubit.metricsflow.metrics.core.mdef;

import com.qubit.metricsflow.core.MetricsFlowOptions;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.MetricUtils;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public abstract class MetricRecorderBase<R extends MetricRecorderBase> implements Serializable {
    private MetricDefinition<R> metricDefinition;
    private DoFn.ProcessContext pctx;
    private List<LabelNameValuePair> labelValues = new LinkedList<>();
    private boolean metricsEnabled;

    public MetricRecorderBase(MetricDefinition<R> metricDefinition, DoFn.ProcessContext pctx) {
        this.metricDefinition = metricDefinition;
        this.pctx = pctx;
        this.metricsEnabled = pctx.getPipelineOptions().as(MetricsFlowOptions.class).getMetricsEnabled();
    }

    public R withLabel(String labelName, String labelValue) {
        labelValues.add(new LabelNameValuePair(labelName, labelValue));
        return (R) this;
    }

    protected void doPush(double value) {
        if (metricsEnabled) {
            KV<MetricUpdateKey, MetricUpdateValue> event = KV.of(
                MetricUpdateKey.of(metricDefinition, labelValues),
                MetricUpdateValue.of(metricDefinition, value)
            );
            pctx.output(MetricUtils.METRICS_TAG, event);
        }
    }
}
