package com.google.cloud.dataflow.sdk.transforms;

import com.qubit.metricsflow.metrics.MetricsBox;
import com.qubit.metricsflow.core.transform.PTransformForParDoBound;
import com.qubit.metricsflow.core.transform.PTransformForParDoBoundMulti;
import com.qubit.metricsflow.core.utils.MetricUtils;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

// This class contains a bunch of hacks transparently adding an extra
// side output tag for metrics collection to DoFns. Once metrics are
// added to a DoFn, it is enough to wrap its parental ParDo block with
// CollectMetrics to enable routing of metric events. The wrapping looks
// as following:
//       CollectMetrics.from(ParDo.of(<instance of a ParDo>)).into(<instance of MetricsBox>)
// CollectMetrics can work with ParDos with existing side outputs.
public class CollectMetrics {
    // for ParDos with no side outputs
    public static <InputT, OutputT> PaDoBoundWrapper<InputT, OutputT> from(ParDo.SingleOutput<InputT, OutputT> parDo) {
         return new PaDoBoundWrapper<>(parDo.withOutputTags(new TupleTag<>(MetricUtils.DEFAULT_STREAM_NAME),
                                                            TupleTagList.of(MetricUtils.METRICS_TAG)));
    }

    // for ParDos with existing side outputs
    public static <InputT, OutputT> ParDoBoundMultiWrapper<InputT, OutputT> from(ParDo.MultiOutput<InputT, OutputT> parDo) {
        TupleTagList sideOutputs = parDo.getAdditionalOutputTags().and(MetricUtils.METRICS_TAG);
        //DoFn<InputT, OutputT> fn, List<PCollectionView<?>> sideInputs, TupleTag<OutputT> mainOutputTag, TupleTagList additionalOutputTags, ItemSpec<? extends Class<?>> fnDisplayData
        ParDo.MultiOutput<InputT, OutputT> boundMulti = ParDo.of(parDo.getFn()).withOutputTags(parDo.getMainOutputTag(), sideOutputs);
        return new ParDoBoundMultiWrapper<>(boundMulti);
    }

    // a wrapper for ParDos with no side outputs
    public static class PaDoBoundWrapper<InputT, OutputT> {
        private ParDo.MultiOutput<InputT, OutputT> boundMulti;

        private PaDoBoundWrapper(ParDo.MultiOutput<InputT, OutputT> boundMulti) {
            this.boundMulti = boundMulti;
        }

        public PTransform<PCollection<InputT>, PCollection<OutputT>> into(MetricsBox mbox) {
            return new PTransformForParDoBound<>(boundMulti, mbox);
        }
    }

    // a wrapper for ParDos with existing side outputs
    public static class ParDoBoundMultiWrapper<InputT, OutputT> {
        private ParDo.MultiOutput<InputT, OutputT> boundMulti;

        private ParDoBoundMultiWrapper(ParDo.MultiOutput<InputT, OutputT> boundMulti) {
            this.boundMulti = boundMulti;
        }

        public PTransform<PCollection<InputT>, PCollectionTuple> into(MetricsBox mbox) {
            return new PTransformForParDoBoundMulti<>(boundMulti, mbox);
        }
    }
}