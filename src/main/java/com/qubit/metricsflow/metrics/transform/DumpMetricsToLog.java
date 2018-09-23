package com.qubit.metricsflow.metrics.transform;

import com.qubit.metricsflow.core.fn.LogMetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class DumpMetricsToLog extends DumpMetricsTransform {
    @Override
    public PDone expand(PCollection<MetricUpdateEvent> input) {
        input.apply(ParDo.of(new LogMetricUpdateEvent()));
        return PDone.in(input.getPipeline());
    }
}
