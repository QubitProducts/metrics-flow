package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.metrics.MetricsBox;
import com.qubit.metricsflow.core.utils.MetricsConsts;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class PTransformForParDoBound<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
    private final MetricsBox mbox;
    private final ParDo.BoundMulti<InputT, OutputT> boundMulti;

    public PTransformForParDoBound(ParDo.BoundMulti<InputT, OutputT> boundMulti, MetricsBox mbox) {
        this.mbox = mbox;
        this.boundMulti = boundMulti;
    }

    @Override
    public PCollection<OutputT> apply(PCollection<InputT> input) {
        PCollectionTuple result = input.apply(boundMulti);
        mbox.add(result.get(MetricsConsts.METRICS_TAG));
        return result.get(new TupleTag<OutputT>(MetricsConsts.DEFAULT_STREAM_NAME));
    }
}
