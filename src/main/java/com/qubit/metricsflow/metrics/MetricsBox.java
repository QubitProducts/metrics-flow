package com.qubit.metricsflow.metrics;

import com.qubit.metricsflow.core.transform.ApplyMetricsFlowTransforms;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.WindowTypeTags;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PDone;

public class MetricsBox {
    private static final String MFLOW_TRANSFORM_NAME = "MetricsFlow";
    private PCollectionList<KV<MetricUpdateKey, MetricUpdateValue>> metricsCollections;

    private MetricsBox(Pipeline pipeline) {
        metricsCollections = PCollectionList.empty(pipeline);
    }

    public static MetricsBox of(Pipeline pipeline) {
        return new MetricsBox(pipeline);
    }

    public void add(PCollection<KV<MetricUpdateKey, MetricUpdateValue>> mCollection) {
        metricsCollections = metricsCollections.and(mCollection);
    }

    public PCollectionTuple apply() {
        return metricsCollections.apply(MFLOW_TRANSFORM_NAME, new ApplyMetricsFlowTransforms());
    }

    public void apply(PTransform<PCollection<MetricUpdateEvent>, PDone> outTransform) {
        PCollectionTuple tpl = metricsCollections.apply(MFLOW_TRANSFORM_NAME, new ApplyMetricsFlowTransforms());
        tpl.get(WindowTypeTags.FIXED_OUT).apply("FixedWindowMetricsOut", outTransform);
        tpl.get(WindowTypeTags.SLIDING_OUT).apply("SlidingWindowMetricsOut", outTransform);
    }
}
