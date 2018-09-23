package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class ApplyFixedWindowAggregations extends ApplyWindowAggregations {
    private Duration windowDuration;
    private Duration allowedLateness;

    public ApplyFixedWindowAggregations(long windowDurationSec, long allowedLatenessSec) {
        this.windowDuration = Duration.standardSeconds(windowDurationSec);
        this.allowedLateness = Duration.standardSeconds(allowedLatenessSec);
    }

    @Override
    public PCollection<MetricUpdateEvent> expand(PCollection<KV<MetricUpdateKey, MetricUpdateValue>> input) {
        return input.apply("CollectInFixedWindow",
            Window.<KV<MetricUpdateKey, MetricUpdateValue>>into(
                FixedWindows.of(windowDuration))
                .withAllowedLateness(allowedLateness)
                .discardingFiredPanes())
            .apply(new ApplyAggregationTransforms(MetricWindowType.Fixed));
    }
}
