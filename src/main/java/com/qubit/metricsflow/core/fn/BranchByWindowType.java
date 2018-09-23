package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.WindowTypeTags;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class BranchByWindowType extends DoFn<KV<MetricUpdateKey, MetricUpdateValue>, Void> {
    private final Counter fixedWindowMetrics = Metrics.counter(BranchByWindowType.class, "FixedWindowMetrics");
    private final Counter slidingWindowMetrics = Metrics.counter(BranchByWindowType.class, "SlidingWindowMetrics");

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
        KV<MetricUpdateKey, MetricUpdateValue> event = processContext.element();
        MetricUpdateValue value = event.getValue();

        if (!value.getFixedWindowAggregations().isEmpty()) {
            sendToFixedWindowStream(processContext, event);
        }
        if (!value.getSlidingWindowAggregations().isEmpty()) {
            sendToSlidingWindowStream(processContext, event);
        }
    }

    private void sendToFixedWindowStream(ProcessContext processContext, KV<MetricUpdateKey, MetricUpdateValue> event) {
        processContext.output(WindowTypeTags.FIXED_IN, event);
        fixedWindowMetrics.inc(1L);
    }

    private void sendToSlidingWindowStream(ProcessContext processContext, KV<MetricUpdateKey, MetricUpdateValue> event) {
        processContext.output(WindowTypeTags.SLIDING_IN, event);
        slidingWindowMetrics.inc(1L);
    }
}
