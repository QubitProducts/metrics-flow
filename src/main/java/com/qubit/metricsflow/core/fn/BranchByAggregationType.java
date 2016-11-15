package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.MetricTypeTags;
import com.qubit.metricsflow.core.utils.MetricUtils;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.EnumSet;

public class BranchByAggregationType extends DoFn<KV<MetricUpdateKey, MetricUpdateValue>, Void> {
    private final Aggregator<Long, Long> numMaxAggrs = createAggregator("NumMaxAggrs", new Sum.SumLongFn());
    private final Aggregator<Long, Long> numMinAggrs = createAggregator("NumMinAggrs", new Sum.SumLongFn());
    private final Aggregator<Long, Long> numMeanAggrs = createAggregator("NumMeanAggrs", new Sum.SumLongFn());
    private final Aggregator<Long, Long> numSumAggrs = createAggregator("NumSumAggrs", new Sum.SumLongFn());

    private MetricWindowType windowType;

    public BranchByAggregationType(MetricWindowType windowType) {
        this.windowType = windowType;
    }

    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        KV<MetricUpdateKey, MetricUpdateValue> event = processContext.element();
        MetricUpdateValue updateValue = event.getValue();
        EnumSet<MetricAggregationType> aggregationsToApply =
            MetricUtils.getAggregationsForWindowType(updateValue, windowType);

        int numAggregationsApplied = 0;
        KV<MetricUpdateKey, Double> outKv = KV.of(event.getKey(), updateValue.getValue());
        if (aggregationsToApply.contains(MetricAggregationType.Max)) {
            processContext.sideOutput(MetricTypeTags.MAX, outKv);
            numMaxAggrs.addValue(1L);
            numAggregationsApplied++;
        }
        if (aggregationsToApply.contains(MetricAggregationType.Min)) {
            processContext.sideOutput(MetricTypeTags.MIN, outKv);
            numMinAggrs.addValue(1L);
            numAggregationsApplied++;
        }
        if (aggregationsToApply.contains(MetricAggregationType.Mean)) {
            processContext.sideOutput(MetricTypeTags.MEAN, outKv);
            numMeanAggrs.addValue(1L);
            numAggregationsApplied++;
        }
        if (aggregationsToApply.contains(MetricAggregationType.Sum)) {
            processContext.sideOutput(MetricTypeTags.SUM, outKv);
            numSumAggrs.addValue(1L);
            numAggregationsApplied++;
        }
        if (numAggregationsApplied == 0) {
            throw new RuntimeException("Do not know how to apply aggregations: " + aggregationsToApply.toString());
        }
    }
}
