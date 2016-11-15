package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.joda.time.Duration;

public class ApplySlidingWindowAggregations extends ApplyWindowAggregations {
    private Duration windowDuration;
    private Duration windowPeriod;

    public ApplySlidingWindowAggregations(long windowDurationSec, long windowPeriodSec) {
        this.windowDuration = Duration.standardSeconds(windowDurationSec);
        this.windowPeriod = Duration.standardSeconds(windowPeriodSec);
    }

    @Override
    public PCollection<MetricUpdateEvent> apply(PCollection<KV<MetricUpdateKey, MetricUpdateValue>> input) {
        return input.apply(
            Window.<KV<MetricUpdateKey, MetricUpdateValue>>into(
                SlidingWindows.of(windowDuration).every(windowPeriod))
                .named("CollectInSlidingWindow")
        ).apply(new ApplyAggregationTransforms(MetricWindowType.Sliding));
    }
}
