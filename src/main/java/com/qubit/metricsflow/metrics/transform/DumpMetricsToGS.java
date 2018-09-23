package com.qubit.metricsflow.metrics.transform;

import com.qubit.metricsflow.core.fn.ConvertMetricUpdateEventToJson;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class DumpMetricsToGS extends DumpMetricsTransform {
    private String gsFilePath;

    public DumpMetricsToGS(String gsFilePath) {
        this.gsFilePath = gsFilePath;
    }

    @Override
    public PDone expand(PCollection<MetricUpdateEvent> input) {
        return input
            .apply(ParDo.of(new ConvertMetricUpdateEventToJson()))
            .apply("DumpMetricsToFile", TextIO.write().to(gsFilePath));
    }
}
