package com.qubit.metricsflow.core.combines;

import com.qubit.metricsflow.core.transform.MapAggregationToMetricUpdateEvent;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class CombineMin extends CombineBase {
    @Override
    public PCollection<MetricUpdateEvent> expand(PCollection<KV<MetricUpdateKey, Double>> input) {
        return input
            .apply(Min.doublesPerKey())
            .apply(new MapAggregationToMetricUpdateEvent(MetricAggregationType.Min));
    }
}
