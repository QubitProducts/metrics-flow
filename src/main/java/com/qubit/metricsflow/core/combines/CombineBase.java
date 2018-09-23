package com.qubit.metricsflow.core.combines;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public abstract class CombineBase extends PTransform<PCollection<KV<MetricUpdateKey, Double>>,
    PCollection<MetricUpdateEvent>> {
}
