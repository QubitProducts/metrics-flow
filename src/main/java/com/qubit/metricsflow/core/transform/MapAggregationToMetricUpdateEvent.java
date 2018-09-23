package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class MapAggregationToMetricUpdateEvent extends PTransform<PCollection<KV<MetricUpdateKey, Double>>,
    PCollection<MetricUpdateEvent>> {

    private String metricNameSuffix;

    public MapAggregationToMetricUpdateEvent(MetricAggregationType aggregationType) {
        metricNameSuffix = MetricAggregationType.toMetricNameSuffix(aggregationType);
    }

    @Override
    public PCollection<MetricUpdateEvent> expand(PCollection<KV<MetricUpdateKey, Double>> input) {
        return input.apply(
            MapElements.into(new TypeDescriptor<MetricUpdateEvent>() {
            }).via((SerializableFunction<KV<MetricUpdateKey, Double>, MetricUpdateEvent>) item -> {
                MetricUpdateKey key = item.getKey();
                String newMetricName = addSuffixToMetricName(key.getMetricName(), metricNameSuffix);
                return new MetricUpdateEvent(newMetricName, key.getLabelNameValuePairs(), item.getValue());
            })
        );
    }

    private static String addSuffixToMetricName(String name, String suffix) {
        return name + "_" + suffix;
    }
}
