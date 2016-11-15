package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.WindowTypeTags;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;

public class BranchByWindowType extends DoFn<KV<MetricUpdateKey, MetricUpdateValue>, Void> {
    private final Aggregator<Long, Long> fixedWindowMetrics = createAggregator("FixedWindowMetrics", new Sum.SumLongFn());
    private final Aggregator<Long, Long> slidingWindowMetrics = createAggregator("SlidingWindowMetrics", new Sum.SumLongFn());

    @Override
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
        processContext.sideOutput(WindowTypeTags.FIXED_IN, event);
        fixedWindowMetrics.addValue(1L);
    }

    private void sendToSlidingWindowStream(ProcessContext processContext, KV<MetricUpdateKey, MetricUpdateValue> event) {
        processContext.sideOutput(WindowTypeTags.SLIDING_IN, event);
        slidingWindowMetrics.addValue(1L);
    }
}
