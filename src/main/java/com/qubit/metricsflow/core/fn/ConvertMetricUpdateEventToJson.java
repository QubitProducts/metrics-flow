package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;

public class ConvertMetricUpdateEventToJson extends DoFn<MetricUpdateEvent, String> {
    private static ObjectMapper mapper = new ObjectMapper();

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
        String result = mapper.writeValueAsString(processContext.element());
        processContext.output(result);
    }
}
