package com.qubit.metricsflow.core;

import com.qubit.metricsflow.core.utils.ImplicitLabelNames;
import com.qubit.metricsflow.core.utils.WindowTypeTags;
import com.qubit.metricsflow.metrics.Counter;
import com.qubit.metricsflow.metrics.Gauge;
import com.qubit.metricsflow.metrics.MetricsBox;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.CollectMetrics;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.LinkedList;

public class MetricsFlowIT {
    private static final String CTR_METRIC_NAME = "my_special_counter";
    private static final String GAUGE_METRIC_NAME = "my_special_gauge";

    private static class OutputWithCurrentTs extends DoFn<String, String> {
        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            processContext.outputWithTimestamp(processContext.element(), Instant.now());
        }
    }

    private static class DoFnWithCounter extends DoFn<String, String> {
        private static Counter numStrings = Counter
            .build()
            .named(CTR_METRIC_NAME)
            .labels("chuck", "doctor")
            .create();

        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            processContext.output(processContext.element());
            numStrings.record(processContext)
                .withLabel("chuck", "norris")
                .withLabel("doctor", "who")
                .inc();
        }
    }

    private static class DoFnWithGauge extends DoFn<String, String> {
        private static Gauge strLen = Gauge
            .build()
            .named(GAUGE_METRIC_NAME)
            .labels("jack", "dull")
            .calculateMovingAverage()
            .calculateMax()
            .calculateMin()
            .create();

        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            String item = processContext.element();
            processContext.output(item);
            strLen.record(processContext)
                .withLabel("jack", "is-a")
                .withLabel("dull", "boy")
                .set(item.length());
        }
    }

    @Test
    public void checkIfItAllWorksTogether_pleaseDoNotBreakIt() {
        runIT(newOptions());
    }

    @Test
    public void checkIfExtraLabelsAre_implicitlyAddedWhenRequired() {
        MetricsFlowOptions options = newOptions();
        options.setIncludeJobNameLabel(true);
        options.setIncludeProjectNameLabel(true);
        options.setProject("testProject");
        options.setJobName("testJob");
        runIT(options);
    }

    private void runIT(MetricsFlowOptions options) {
        Pipeline p = TestPipeline.create(options);
        MetricsBox mbox = MetricsBox.of(p);

        p.apply(Create.of("s1x", "s2xx", "s3xxx", "s4xxxx"))
            .apply(ParDo.of(new OutputWithCurrentTs()))
            .apply(CollectMetrics.from(ParDo.of(new DoFnWithCounter())).into(mbox))
            .apply(CollectMetrics.from(ParDo.of(new DoFnWithGauge())).into(mbox));

        PCollectionTuple result = mbox.apply();
        PCollection<MetricUpdateEvent> fixedWindowResults = result.get(WindowTypeTags.FIXED_OUT);
        PCollection<MetricUpdateEvent> slidingWindowEvents = result.get(WindowTypeTags.SLIDING_OUT);

        MetricUpdateEvent
            counterUpdateEvent = createMUEvent(options, CTR_METRIC_NAME + "_counter", 4,
                                               "chuck=norris", "doctor=who");
        MetricUpdateEvent gaugeMaxEvent = createMUEvent(options, GAUGE_METRIC_NAME + "_max_gauge",
                                                        6, "jack=is-a", "dull=boy");
        MetricUpdateEvent gaugeMinEvent = createMUEvent(options, GAUGE_METRIC_NAME + "_min_gauge", 3,
                                                        "jack=is-a", "dull=boy");
        MetricUpdateEvent gaugeMavgEvent = createMUEvent(options, GAUGE_METRIC_NAME + "_mean_gauge",
                                                         4.5, "jack=is-a", "dull=boy");

        DataflowAssert.that(fixedWindowResults).containsInAnyOrder(counterUpdateEvent, gaugeMaxEvent, gaugeMinEvent);
        DataflowAssert.that(slidingWindowEvents).containsInAnyOrder(gaugeMavgEvent, gaugeMavgEvent);

        p.run();
    }

    private MetricUpdateEvent createMUEvent(MetricsFlowOptions options, String name,
                                            double value, String... kvStrings) {
        LinkedList<LabelNameValuePair> nvPairs = new LinkedList<>();

        for (String kv : kvStrings) {
            String[] items = kv.split("=");
            if (items.length != 2) {
                throw new RuntimeException("Bad key-value pair: " + kv);
            }

            nvPairs.addLast(new LabelNameValuePair(items[0], items[1]));
        }
        if (options.getIncludeProjectNameLabel()) {
            nvPairs.add(new LabelNameValuePair(ImplicitLabelNames.GCP_PROJECT_NAME, options.getProject()));
        }
        if (options.getIncludeJobNameLabel()) {
            nvPairs.add(new LabelNameValuePair(ImplicitLabelNames.GCP_JOB_NAME, options.getJobName()));
        }

        nvPairs.sort(LabelNameValuePair::compareTo);
        return new MetricUpdateEvent(name, nvPairs, value);
    }

    private static MetricsFlowOptions newOptions() {
        MetricsFlowOptions options = PipelineOptionsFactory.create().as(MetricsFlowOptions.class);
        options.setSlidingWindowDurationSec(10);
        options.setSlidingWindowPeriodSec(5);
        return options;
    }
}