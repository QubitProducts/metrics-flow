package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.metrics.MetricsBox;
import com.qubit.metricsflow.core.utils.MetricUtils;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;

public class PTransformForParDoBoundMulti<InputT, OutputT> extends PTransform<PCollection<InputT>, PCollectionTuple> {
    private final MetricsBox mbox;
    private final ParDo.BoundMulti<InputT, OutputT> boundMulti;

    public PTransformForParDoBoundMulti(ParDo.BoundMulti<InputT, OutputT> boundMulti, MetricsBox mbox) {
        super(boundMulti.getName());
        this.boundMulti = boundMulti;
        this.mbox = mbox;
    }

    @Override
    public PCollectionTuple apply(PCollection<InputT> input) {
        PCollectionTuple result = input.apply(boundMulti);
        mbox.add(result.get(MetricUtils.METRICS_TAG));
        return result;
    }
}
