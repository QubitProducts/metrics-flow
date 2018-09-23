package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.MetricTypeTags;
import com.qubit.metricsflow.core.utils.MetricUtils;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.EnumSet;

public class BranchByAggregationType extends DoFn<KV<MetricUpdateKey, MetricUpdateValue>, Void> {
    private final Counter numMaxAggrs = Metrics.counter(BranchByAggregationType.class, "NumMaxAggrs");
    private final Counter numMinAggrs = Metrics.counter(BranchByAggregationType.class, "NumMinAggrs");
    private final Counter numMeanAggrs = Metrics.counter(BranchByAggregationType.class, "NumMeanAggrs");
    private final Counter numSumAggrs = Metrics.counter(BranchByAggregationType.class, "NumSumAggrs");

    private MetricWindowType windowType;

    public BranchByAggregationType(MetricWindowType windowType) {
        this.windowType = windowType;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
        KV<MetricUpdateKey, MetricUpdateValue> event = processContext.element();
        MetricUpdateValue updateValue = event.getValue();
        EnumSet<MetricAggregationType> aggregationsToApply =
            MetricUtils.getAggregationsForWindowType(updateValue, windowType);

        int numAggregationsApplied = 0;
        KV<MetricUpdateKey, Double> outKv = KV.of(event.getKey(), updateValue.getValue());
        if (aggregationsToApply.contains(MetricAggregationType.Max)) {
            processContext.output(MetricTypeTags.MAX, outKv);
            numMaxAggrs.inc(1L);
            numAggregationsApplied++;
        }
        if (aggregationsToApply.contains(MetricAggregationType.Min)) {
            processContext.output(MetricTypeTags.MIN, outKv);
            numMinAggrs.inc(1L);
            numAggregationsApplied++;
        }
        if (aggregationsToApply.contains(MetricAggregationType.Mean)) {
            processContext.output(MetricTypeTags.MEAN, outKv);
            numMeanAggrs.inc(1L);
            numAggregationsApplied++;
        }
        if (aggregationsToApply.contains(MetricAggregationType.Sum)) {
            processContext.output(MetricTypeTags.SUM, outKv);
            numSumAggrs.inc(1L);
            numAggregationsApplied++;
        }
        if (numAggregationsApplied == 0) {
            throw new RuntimeException("Do not know how to apply aggregations: " + aggregationsToApply.toString());
        }
    }
}
