package com.qubit.metricsflow.metrics;

import com.qubit.metricsflow.core.MetricsFlowOptions;
import com.qubit.metricsflow.core.fn.BranchByWindowType;
import com.qubit.metricsflow.core.fn.MapToMetricKeyValuePairs;
import com.qubit.metricsflow.core.transform.ApplyFixedWindowAggregations;
import com.qubit.metricsflow.core.transform.ApplySlidingWindowAggregations;
import com.qubit.metricsflow.core.utils.WindowTypeTags;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import java.util.Arrays;

public class MetricsBox {
    private PCollectionList<MetricUpdateEvent> metricsCollections;

    protected MetricsBox(Pipeline pipeline) {
        metricsCollections = PCollectionList.empty(pipeline);
    }

    public static MetricsBox of(Pipeline pipeline) {
        return new MetricsBox(pipeline);
    }

    public void add(PCollection<MetricUpdateEvent> mCollection) {
        metricsCollections = metricsCollections.and(mCollection);
    }

    public PCollection<MetricUpdateEvent> getAllEvents() {
        return metricsCollections.apply(Flatten.pCollections());
    }

    public PCollectionTuple applyAggregations(MetricsFlowOptions options) {
        PCollectionTuple results = getEventsSplitByWindowType();
        return PCollectionTuple.empty(results.getPipeline())
            .and(WindowTypeTags.FIXED_OUT,
                results.get(WindowTypeTags.FIXED_IN)
                    .apply(new ApplyFixedWindowAggregations(options.getFixedWindowDurationSec(),
                                                            options.getFixedWindowAllowedLatenessSec())))
            .and(WindowTypeTags.SLIDING_OUT,
                results.get(WindowTypeTags.SLIDING_IN)
                    .apply(new ApplySlidingWindowAggregations(options.getSlidingWindowDurationSec(),
                                                              options.getSlidingWindowPeriodSec()))
            );
    }

    public void emptyBoxToPubSub(MetricsFlowOptions options) {
        PCollectionTuple results = applyAggregations(options);
        writeResultsToPubSub(results.get(WindowTypeTags.FIXED_OUT), options);
        writeResultsToPubSub(results.get(WindowTypeTags.SLIDING_OUT), options);
    }

    private void writeResultsToPubSub(PCollection<MetricUpdateEvent> results, MetricsFlowOptions options) {
        results
            .apply(
                PubsubIO.Write.topic(options.getOutputTopicName())
                .named("WriteMetricsToPubSub")
                .withCoder(AvroCoder.of(MetricUpdateEvent.class))
            );
    }

    private PCollectionTuple getEventsSplitByWindowType() {
        return getAllEvents()
            .apply(ParDo.of(new MapToMetricKeyValuePairs()).named("MapToMetricKVPairs"))
            .apply(ParDo.of(new BranchByWindowType())
                       .named("BranchByWindowType")
                       .withOutputTags(new TupleTag<>("NoDefaultOutput"),
                                       TupleTagList.of(
                                           Arrays.asList(WindowTypeTags.FIXED_IN, WindowTypeTags.SLIDING_IN))));
    }
}
