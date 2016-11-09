package com.qubit.metricsflow.metrics.core.mdef;

import com.qubit.metricsflow.core.utils.MetricsConsts;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

import java.util.LinkedList;
import java.util.List;

public abstract class MetricRecorderBase<R extends MetricRecorderBase> {
    private String metricName;
    private DoFn.ProcessContext pctx;
    private List<LabelNameValuePair> labelValues = new LinkedList<>();

    public MetricRecorderBase(String metricName, DoFn.ProcessContext pctx) {
        this.metricName = metricName;
        this.pctx = pctx;
    }

    public R withLabel(String labelName, String labelValue) {
        labelValues.add(new LabelNameValuePair(labelName, labelValue));
        return (R) this;
    }

    protected void doPush(double value) {
        MetricUpdateEvent event = new MetricUpdateEvent(metricName, labelValues, value);
        pctx.sideOutput(MetricsConsts.METRICS_TAG, event);
    }
}
