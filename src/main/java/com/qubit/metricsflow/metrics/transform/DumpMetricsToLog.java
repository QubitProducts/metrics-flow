package com.qubit.metricsflow.metrics.transform;

import com.qubit.metricsflow.core.fn.LogMetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

public class DumpMetricsToLog extends DumpMetricsTransform {
    @Override
    public PDone apply(PCollection<MetricUpdateEvent> input) {
        input.apply(ParDo.of(new LogMetricUpdateEvent()));
        return PDone.in(input.getPipeline());
    }
}
