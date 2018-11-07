package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.utils.MetricUtils;
import com.qubit.metricsflow.metrics.MetricsBox;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

public class PTransformForParDoBoundMulti<InputT, OutputT> extends PTransform<PCollection<InputT>, PCollectionTuple> {
    private final MetricsBox mbox;
    private final ParDo.MultiOutput<InputT, OutputT> boundMulti;

    public PTransformForParDoBoundMulti(ParDo.MultiOutput<InputT, OutputT> boundMulti, MetricsBox mbox) {
        super(boundMulti.getName());
        this.boundMulti = boundMulti;
        this.mbox = mbox;
    }

    @Override
    public PCollectionTuple expand(PCollection<InputT> input) {
        PCollectionTuple result = input.apply(boundMulti);
        mbox.add(result.get(MetricUtils.METRICS_TAG));
        return result;
    }
}
