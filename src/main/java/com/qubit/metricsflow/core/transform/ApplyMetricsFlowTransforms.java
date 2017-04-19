package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.MetricsFlowOptions;
import com.qubit.metricsflow.core.fn.BranchByWindowType;
import com.qubit.metricsflow.core.fn.VerifyMetricKey;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.WindowTypeTags;

import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import java.util.Arrays;

public class ApplyMetricsFlowTransforms
    extends PTransform<PCollectionList<KV<MetricUpdateKey, MetricUpdateValue>>, PCollectionTuple> {

    @Override
    public PCollectionTuple apply(PCollectionList<KV<MetricUpdateKey, MetricUpdateValue>> input) {
        MetricsFlowOptions options = input.getPipeline().getOptions().as(MetricsFlowOptions.class);

        PCollectionTuple results = input.apply(Flatten.pCollections())
            .apply(ParDo.of(new VerifyMetricKey())
                       .named("VerifyMetricKey"))
            .apply(ParDo.of(new BranchByWindowType())
                       .named("BranchByWindowType")
                       .withOutputTags(new TupleTag<>("NoDefaultOutput"),
                                       TupleTagList.of(
                                           Arrays.asList(WindowTypeTags.FIXED_IN,
                                                         WindowTypeTags.SLIDING_IN))));

        return PCollectionTuple.empty(results.getPipeline())
            .and(WindowTypeTags.FIXED_OUT,
                 results.get(WindowTypeTags.FIXED_IN)
                     .apply(new ApplyFixedWindowAggregations(options.getFixedWindowDurationSec(),
                                                             options.getFixedWindowAllowedLatenessSec()))
                     .apply(new IncludeExtraLabels())
            )
            .and(WindowTypeTags.SLIDING_OUT,
                 results.get(WindowTypeTags.SLIDING_IN)
                     .apply(new ApplySlidingWindowAggregations(options.getSlidingWindowDurationSec(),
                                                               options.getSlidingWindowPeriodSec()))
                     .apply(new IncludeExtraLabels())
            );
    }
}
