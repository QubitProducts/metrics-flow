package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.core.combines.CombineMax;
import com.qubit.metricsflow.core.combines.CombineMean;
import com.qubit.metricsflow.core.combines.CombineMin;
import com.qubit.metricsflow.core.combines.CombineSum;
import com.qubit.metricsflow.core.fn.BranchByAggregationType;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.utils.MetricTypeTags;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class ApplyAggregationTransforms extends PTransform<PCollection<KV<MetricUpdateKey, Double>>,
    PCollection<MetricUpdateEvent>> {
    private MetricWindowType windowType;

    public ApplyAggregationTransforms(MetricWindowType windowType) {
        super("ApplyAggregations");
        this.windowType = windowType;
    }

    @Override
    public PCollection<MetricUpdateEvent> apply(
        PCollection<KV<MetricUpdateKey, Double>> input) {

        PCollectionTuple result = input.apply(
            ParDo.of(new BranchByAggregationType(windowType))
                .named("BranchByAggregationType")
                .withOutputTags(new TupleTag<>("NoDefaultOutput"),
                                MetricTypeTags.getTupleTagList()));

        return PCollectionList
            .<MetricUpdateEvent>empty(input.getPipeline())
            .and(result.get(MetricTypeTags.MIN).apply(new CombineMin()))
            .and(result.get(MetricTypeTags.MAX).apply(new CombineMax()))
            .and(result.get(MetricTypeTags.SUM).apply(new CombineSum()))
            .and(result.get(MetricTypeTags.MEAN).apply(new CombineMean()))
            .apply(Flatten.pCollections());
    }

}
