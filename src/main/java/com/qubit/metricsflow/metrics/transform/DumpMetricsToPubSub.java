package com.qubit.metricsflow.metrics.transform;

import com.qubit.metricsflow.core.MetricsFlowOptions;
import com.qubit.metricsflow.core.fn.ConvertMetricUpdateEventToJson;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

public class DumpMetricsToPubSub extends DumpMetricsTransform {
    @Override
    public PDone apply(PCollection<MetricUpdateEvent> input) {
        MetricsFlowOptions options = input.getPipeline().getOptions().as(MetricsFlowOptions.class);
        return input
            .apply(ParDo.of(new ConvertMetricUpdateEventToJson()))
            .apply(PubsubIO.Write.topic(options.getMetricsOutputTopicName()));
    }
}
