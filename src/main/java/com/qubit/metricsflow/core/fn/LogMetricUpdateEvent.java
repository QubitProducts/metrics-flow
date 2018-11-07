package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogMetricUpdateEvent extends DoFn<MetricUpdateEvent, MetricUpdateEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(LogMetricUpdateEvent.class);
    private static ObjectMapper mapper = new ObjectMapper();

    public LogMetricUpdateEvent() {
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
        MetricUpdateEvent event = processContext.element();
        LOG.info(mapper.writeValueAsString(event));
        processContext.output(event);
    }
}
