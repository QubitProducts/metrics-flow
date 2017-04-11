package com.qubit.metricsflow.metrics.transform;

import com.qubit.metricsflow.core.fn.ConvertMetricUpdateEventToJson;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

public class DumpMetricsToGS extends DumpMetricsTransform {
    private String gsFilePath;

    public DumpMetricsToGS(String gsFilePath) {
        this.gsFilePath = gsFilePath;
    }

    @Override
    public PDone apply(PCollection<MetricUpdateEvent> input) {
        return input
            .apply(ParDo.of(new ConvertMetricUpdateEventToJson()))
            .apply(TextIO.Write.named("DumpMetricsToFile").to(gsFilePath));
    }
}
