package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public abstract class ApplyWindowAggregations extends PTransform<PCollection<KV<MetricUpdateKey, MetricUpdateValue>>,
    PCollection<MetricUpdateEvent>> {
}
