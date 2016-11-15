package com.google.cloud.dataflow.sdk.transforms;

import com.qubit.metricsflow.metrics.MetricsBox;
import com.qubit.metricsflow.core.transform.PTransformForParDoBound;
import com.qubit.metricsflow.core.transform.PTransformForParDoBoundMulti;
import com.qubit.metricsflow.core.utils.MetricUtils;

import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

public class CollectMetrics {
    public static <InputT, OutputT> PaDoBoundWrapper<InputT, OutputT> from(ParDo.Bound<InputT, OutputT> parDo) {
         return new PaDoBoundWrapper<>(parDo.withOutputTags(new TupleTag<>(MetricUtils.DEFAULT_STREAM_NAME),
                                                            TupleTagList.of(MetricUtils.METRICS_TAG)));
    }

    public static <InputT, OutputT> ParDoBoundMultiWrapper<InputT, OutputT> from(ParDo.BoundMulti<InputT, OutputT> parDo) {
        TupleTagList sideOutputs = parDo.getSideOutputTags().and(MetricUtils.METRICS_TAG);
        ParDo.BoundMulti<InputT, OutputT> boundMulti = new ParDo.BoundMulti<>(parDo.getName(), parDo.getSideInputs(),
            parDo.getMainOutputTag(), sideOutputs, parDo.getFn(),
            parDo.getFn().getClass());
        return new ParDoBoundMultiWrapper<>(boundMulti);
    }

    public static class PaDoBoundWrapper<InputT, OutputT> {
        private ParDo.BoundMulti<InputT, OutputT> boundMulti;

        private PaDoBoundWrapper(ParDo.BoundMulti<InputT, OutputT> boundMulti) {
            this.boundMulti = boundMulti;
        }

        public PTransform<PCollection<InputT>, PCollection<OutputT>> into(MetricsBox mbox) {
            return new PTransformForParDoBound<>(boundMulti, mbox);
        }
    }

    public static class ParDoBoundMultiWrapper<InputT, OutputT> {
        private ParDo.BoundMulti<InputT, OutputT> boundMulti;

        private ParDoBoundMultiWrapper(ParDo.BoundMulti<InputT, OutputT> boundMulti) {
            this.boundMulti = boundMulti;
        }

        public PTransform<PCollection<InputT>, PCollectionTuple> into(MetricsBox mbox) {
            return new PTransformForParDoBoundMulti<>(boundMulti, mbox);
        }
    }
}