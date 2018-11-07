package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.fn.BranchByWindowType;
import com.qubit.metricsflow.core.fn.VerifyMetricKey;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.WindowTypeTags;

import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Arrays;

public class ApplyMetricsFlowTransforms
    extends PTransform<PCollectionList<KV<MetricUpdateKey, MetricUpdateValue>>, PCollectionTuple> {

    private final String project;
    private final String jobName;
    private final Integer fixedWindowDurationSec;
    private final Integer fixedWindowAllowedLatenessSec;
    private final Integer slidingWindowDurationSec;
    private final Integer slidingWindowPeriodicSec;

    public ApplyMetricsFlowTransforms(String project, String jobName, Integer fixedWindowDurationSec,
                                      Integer fixedWindowAllowedLatenessSec,
                                      Integer slidingWindowDurationSec,
                                      Integer slidingWindowPeriodicSec) {
        this.project = project;
        this.jobName = jobName;
        this.fixedWindowDurationSec = fixedWindowDurationSec;
        this.fixedWindowAllowedLatenessSec = fixedWindowAllowedLatenessSec;
        this.slidingWindowDurationSec = slidingWindowDurationSec;
        this.slidingWindowPeriodicSec = slidingWindowPeriodicSec;
    }

    @Override
    public PCollectionTuple expand(PCollectionList<KV<MetricUpdateKey, MetricUpdateValue>> input) {

        PCollectionTuple results = input.apply(Flatten.pCollections())
            .apply("VerifyMetricKey", ParDo.of(new VerifyMetricKey()))
            .apply("BranchByWindowType", ParDo.of(new BranchByWindowType())
                .withOutputTags(new TupleTag<>("NoDefaultOutput"),
                                TupleTagList.of(
                                    Arrays.asList(WindowTypeTags.FIXED_IN,
                                                  WindowTypeTags.SLIDING_IN))));

        return PCollectionTuple.empty(results.getPipeline())
            .and(WindowTypeTags.FIXED_OUT,
                 results.get(WindowTypeTags.FIXED_IN)
                     .apply(new ApplyFixedWindowAggregations(fixedWindowDurationSec, fixedWindowAllowedLatenessSec))
                     .apply(new IncludeExtraLabels(project, jobName))
            )
            .and(WindowTypeTags.SLIDING_OUT,
                 results.get(WindowTypeTags.SLIDING_IN)
                     .apply(
                         new ApplySlidingWindowAggregations(slidingWindowDurationSec, slidingWindowPeriodicSec))
                     .apply(new IncludeExtraLabels(project, jobName))
            );
    }
}
