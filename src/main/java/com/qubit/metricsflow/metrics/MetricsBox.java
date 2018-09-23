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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;

public class MetricsBox {
    private static final String MFLOW_TRANSFORM_NAME = "MetricsFlow";
    private MetricsFlowOptions options;
    private PCollectionList<KV<MetricUpdateKey, MetricUpdateValue>> metricsCollections;

    private MetricsBox(Pipeline pipeline, MetricsFlowOptions options) {
        metricsCollections = PCollectionList.empty(pipeline);
        this.options = options;
    }

    public static MetricsBox of(Pipeline pipeline, MetricsFlowOptions options) {
        return new MetricsBox(pipeline, options);
    }

    public void add(PCollection<KV<MetricUpdateKey, MetricUpdateValue>> mCollection) {
        metricsCollections = metricsCollections.and(mCollection);
    }

    public PCollectionTuple apply() {
        String projectName = options.getIncludeProjectNameLabel() ? options.getProject() : null;
        String jobName = options.getIncludeJobNameLabel() ? options.getJobName() : null;

        return metricsCollections.apply(MFLOW_TRANSFORM_NAME,
                                        new ApplyMetricsFlowTransforms(projectName, jobName,
                                                                       options.getFixedWindowDurationSec(),
                                                                       options.getFixedWindowAllowedLatenessSec(),
                                                                       options.getSlidingWindowDurationSec(),
                                                                       options.getSlidingWindowPeriodSec()));
    }

    public void run() {
        DumpMetricsTransform outTransform = getTransformFromOptions();
        String projectName = options.getIncludeProjectNameLabel() ? options.getProject() : null;
        String jobName = options.getIncludeJobNameLabel() ? options.getJobName() : null;

        PCollectionTuple tpl = metricsCollections.apply(MFLOW_TRANSFORM_NAME, new ApplyMetricsFlowTransforms(
            projectName, jobName, options.getFixedWindowDurationSec(), options.getFixedWindowAllowedLatenessSec(),
            options.getSlidingWindowDurationSec(), options.getSlidingWindowDurationSec()));
        tpl.get(WindowTypeTags.FIXED_OUT)
            .apply("FixedWindowMetricsOut", outTransform);
        tpl.get(WindowTypeTags.SLIDING_OUT)
            .apply("SlidingWindowMetricsOut", outTransform);
    }

    private DumpMetricsTransform getTransformFromOptions() {
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
