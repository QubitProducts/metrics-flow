package com.qubit.metricsflow.metrics.transform;

import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public abstract class DumpMetricsTransform extends PTransform<PCollection<MetricUpdateEvent>, PDone> {
    public abstract PDone expand(PCollection<MetricUpdateEvent> input);
}
