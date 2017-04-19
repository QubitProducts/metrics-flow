package com.qubit.metricsflow.metrics;

import com.qubit.metricsflow.core.MetricsFlowOptions;
import com.qubit.metricsflow.core.transform.ApplyMetricsFlowTransforms;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.WindowTypeTags;
import com.qubit.metricsflow.metrics.transform.DumpMetricsToGS;
import com.qubit.metricsflow.metrics.transform.DumpMetricsToLog;
import com.qubit.metricsflow.metrics.transform.DumpMetricsToPubSub;
import com.qubit.metricsflow.metrics.transform.DumpMetricsTransform;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;

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

    public void run() {
        DumpMetricsTransform outTransform = getTransformFromOptions();
        PCollectionTuple tpl = metricsCollections.apply(MFLOW_TRANSFORM_NAME, new ApplyMetricsFlowTransforms());
        tpl.get(WindowTypeTags.FIXED_OUT)
            .apply("FixedWindowMetricsOut", outTransform);
        tpl.get(WindowTypeTags.SLIDING_OUT)
            .apply("SlidingWindowMetricsOut", outTransform);
    }

    private DumpMetricsTransform getTransformFromOptions() {
        MetricsFlowOptions options =  metricsCollections.getPipeline()
            .getOptions().as(MetricsFlowOptions.class);

        String resourceName = options.getMetricsOutputResourceName();
        if (resourceName.startsWith("pubsub://")) {
            return new DumpMetricsToPubSub(resourceName.substring(9, resourceName.length()));
        } else if (resourceName.startsWith("gs://")) {
            return new DumpMetricsToGS(resourceName.substring(5, resourceName.length()));
        } else if (resourceName.equals("log")) {
            return new DumpMetricsToLog();
        } else {
            throw new RuntimeException("Unsupported resource type: " + resourceName);
        }
    }
}
