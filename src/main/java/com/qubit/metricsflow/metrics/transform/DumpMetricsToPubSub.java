package com.qubit.metricsflow.metrics.transform;

import com.qubit.metricsflow.core.fn.ConvertMetricUpdateEventToJson;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class DumpMetricsToPubSub extends DumpMetricsTransform {
    private String destTopicName;

    public DumpMetricsToPubSub(String destTopicName) {
        this.destTopicName = destTopicName;
    }

    @Override
    public PDone expand(PCollection<MetricUpdateEvent> input) {
        return input
            .apply(ParDo.of(new ConvertMetricUpdateEventToJson()))
            .apply(PubsubIO.writeStrings().to(destTopicName));
    }
}
