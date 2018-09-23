package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class ApplySlidingWindowAggregations extends ApplyWindowAggregations {
    private Duration windowDuration;
    private Duration windowPeriod;

    public ApplySlidingWindowAggregations(long windowDurationSec, long windowPeriodSec) {
        this.windowDuration = Duration.standardSeconds(windowDurationSec);
        this.windowPeriod = Duration.standardSeconds(windowPeriodSec);
    }

    @Override
    public PCollection<MetricUpdateEvent> expand(PCollection<KV<MetricUpdateKey, MetricUpdateValue>> input) {
        return input.apply("CollectInSlidingWindow",
                           Window.into(SlidingWindows.of(windowDuration).every(windowPeriod))
        ).apply(new ApplyAggregationTransforms(MetricWindowType.Sliding));
    }
}
