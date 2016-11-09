package com.qubit.metricsflow.core.combines;

import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.core.transform.MapAggregationToMetricUpdateEvent;
import com.qubit.metricsflow.core.types.MetricUpdateKey;

import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class CombineMax extends CombineBase {
    @Override
    public PCollection<MetricUpdateEvent> apply(PCollection<KV<MetricUpdateKey, Double>> input) {
        return input
            .apply(Max.doublesPerKey())
            .apply(new MapAggregationToMetricUpdateEvent(MetricAggregationType.Max));
    }
}
