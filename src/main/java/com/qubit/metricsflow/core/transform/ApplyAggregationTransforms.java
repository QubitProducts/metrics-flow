package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.combines.CombineMax;
import com.qubit.metricsflow.core.combines.CombineMean;
import com.qubit.metricsflow.core.combines.CombineMin;
import com.qubit.metricsflow.core.combines.CombineSum;
import com.qubit.metricsflow.core.fn.BranchByAggregationType;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.MetricTypeTags;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.types.MetricWindowType;

import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

public class ApplyAggregationTransforms extends PTransform<PCollection<KV<MetricUpdateKey, MetricUpdateValue>>,
    PCollection<MetricUpdateEvent>> {
    private MetricWindowType windowType;

    public ApplyAggregationTransforms(MetricWindowType windowType) {
        super("ApplyAggregations");
        this.windowType = windowType;
    }

    @Override
    public PCollection<MetricUpdateEvent> expand(PCollection<KV<MetricUpdateKey, MetricUpdateValue>> input) {

        PCollectionTuple result = input.apply("BranchByAggregationType",
            ParDo.of(new BranchByAggregationType(windowType))
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
