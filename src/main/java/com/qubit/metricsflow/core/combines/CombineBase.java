package com.qubit.metricsflow.core.combines;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public abstract class CombineBase extends PTransform<PCollection<KV<MetricUpdateKey, Double>>,
    PCollection<MetricUpdateEvent>> {
}
