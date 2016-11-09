package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.joda.time.Duration;

public class ApplyFixedWindowAggregations extends ApplyWindowAggregations {
    private Duration windowDuration;
    private Duration allowedLateness;

    public ApplyFixedWindowAggregations(long windowDurationSec, long allowedLatenessSec) {
        this.windowDuration = Duration.standardSeconds(windowDurationSec);
        this.allowedLateness = Duration.standardSeconds(allowedLatenessSec);
    }

    @Override
    public PCollection<MetricUpdateEvent> apply(PCollection<KV<MetricUpdateKey, Double>> input) {
        return input.apply(
            Window.<KV<MetricUpdateKey, Double>>into(
                FixedWindows.of(windowDuration))
                .named("CollectInFixedWindow")
                .withAllowedLateness(allowedLateness)
                .discardingFiredPanes())
            .apply(new ApplyAggregationTransforms(MetricWindowType.Fixed));
    }
}
