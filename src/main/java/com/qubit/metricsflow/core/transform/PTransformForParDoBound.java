package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.metrics.MetricsBox;
import com.qubit.metricsflow.core.utils.MetricUtils;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

public class PTransformForParDoBound<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
    private final MetricsBox mbox;
    private final ParDo.MultiOutput<InputT, OutputT> boundMulti;

    public PTransformForParDoBound(ParDo.MultiOutput<InputT, OutputT> boundMulti, MetricsBox mbox) {
        super(boundMulti.getName());
        this.mbox = mbox;
        this.boundMulti = boundMulti;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> input) {
        PCollectionTuple result = input.apply(boundMulti);
        mbox.add(result.get(MetricUtils.METRICS_TAG));
        return result.get(new TupleTag<OutputT>(MetricUtils.DEFAULT_STREAM_NAME));
    }
}
