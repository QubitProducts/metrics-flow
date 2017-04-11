package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

public class MapAggregationToMetricUpdateEvent extends PTransform<PCollection<KV<MetricUpdateKey, Double>>,
    PCollection<MetricUpdateEvent>> {

    private String metricNameSuffix;

    public MapAggregationToMetricUpdateEvent(MetricAggregationType aggregationType) {
        metricNameSuffix = MetricAggregationType.toMetricNameSuffix(aggregationType);
    }

    @Override
    public PCollection<MetricUpdateEvent> apply(PCollection<KV<MetricUpdateKey, Double>> input) {
        return input.apply(
            MapElements.via((KV<MetricUpdateKey, Double> item) -> {
                MetricUpdateKey key = item.getKey();
                String newMetricName = addSuffixToMetricName(key.getMetricName(), metricNameSuffix);
                return new MetricUpdateEvent(newMetricName, key.getLabelNameValuePairs(), item.getValue());
            }).withOutputType(new TypeDescriptor<MetricUpdateEvent>() {})
        );
    }

    private static String addSuffixToMetricName(String name, String suffix) {
        return name + "_" + suffix;
    }
}
