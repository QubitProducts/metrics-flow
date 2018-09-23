package com.qubit.metricsflow.core.combines;

import com.qubit.metricsflow.core.transform.MapAggregationToMetricUpdateEvent;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class CombineMean extends CombineBase {
    @Override
    public PCollection<MetricUpdateEvent> expand(PCollection<KV<MetricUpdateKey, Double>> input) {
        return input
            .apply(Mean.perKey())
            .apply(new MapAggregationToMetricUpdateEvent(MetricAggregationType.Mean));
    }
}
