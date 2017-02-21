package com.qubit.metricsflow.metrics.transform;

import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

public abstract class DumpMetricsTransform extends PTransform<PCollection<MetricUpdateEvent>, PDone> {
    @Override
    public abstract PDone apply(PCollection<MetricUpdateEvent> input);
}
