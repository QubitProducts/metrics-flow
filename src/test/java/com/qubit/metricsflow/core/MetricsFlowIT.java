package com.qubit.metricsflow.core;

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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class MetricsFlowIT {
    private static final String CTR_METRIC_NAME = "my_special_counter";
    private static final String GAUGE_METRIC_NAME = "my_special_gauge";

    private static final Logger LOG = LoggerFactory.getLogger(MetricsFlowIT.class);
    private static MetricsFlowOptions options = PipelineOptionsFactory.create().as(MetricsFlowOptions.class);

    private static class DoFnWithCounter extends DoFn<String, String> {
        private static Counter numStrings = Counter
            .build()
            .named(CTR_METRIC_NAME)
            .labels("chuck", "doctor")
            .register();

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
            .register();

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

    public static class IdentityDoFn extends DoFn<MetricUpdateEvent, MetricUpdateEvent> {

        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            MetricUpdateEvent event = processContext.element();
            LOG.info("Metric {} => {} ... ", event.getName(), event.getValue());
            LOG.info("  Labels:");
            event.getLabelNameValuePairs().forEach(nvPair -> {
                LOG.info("    {} -> {}", nvPair.getName(), nvPair.getValue());
            });

            processContext.output(event);
        }
    }

    private static class TestDoFn extends DoFn<MetricUpdateEvent, MetricUpdateEvent> {

        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            MetricUpdateEvent in = processContext.element();
            MetricUpdateEvent counterUpdateEvent = createMUEvent(CTR_METRIC_NAME + "_sum", 4, "chuck=norris", "doctor=who");
            LOG.info("==========================>>>>>> {}", in.equals(counterUpdateEvent));
            processContext.output(processContext.element());
        }

        private MetricUpdateEvent createMUEvent(String name, double value, String... kvStrings) {
            LinkedList<LabelNameValuePair> nvPairs = new LinkedList<>();

            for (String kv : kvStrings) {
                String[] items = kv.split("=");
                if (items.length != 2) {
                    throw new RuntimeException("Bad key-value pair: " + kv);
                }

                nvPairs.addLast(new LabelNameValuePair(items[0], items[1]));
            }

            nvPairs.sort(LabelNameValuePair::compareTo);
            return new MetricUpdateEvent(name, nvPairs, value);
        }
    }

    @Test
    public void checkThatDifferentMetricsWorkWellTogether() {
        Pipeline p = TestPipeline.create(options);
        MetricsBox mbox = MetricsBox.of(p);

        p.apply(Create.of("s1x", "s2xx", "s3xxx", "s4xxxx"))
            .apply(CollectMetrics.from(ParDo.of(new DoFnWithCounter())).into(mbox))
            .apply(CollectMetrics.from(ParDo.of(new DoFnWithGauge())).into(mbox));

        PCollectionTuple result = mbox.applyAggregations(options);
        PCollection<MetricUpdateEvent> fixedWindowResults = result.get(WindowTypeTags.FIXED_OUT);
        PCollection<MetricUpdateEvent> slidingWindowEvents = result.get(WindowTypeTags.SLIDING_OUT)
            .apply(ParDo.of(new IdentityDoFn()));

        MetricUpdateEvent counterUpdateEvent = createMUEvent(CTR_METRIC_NAME + "_sum", 4, "chuck=norris", "doctor=who");
        MetricUpdateEvent gaugeMaxEvent = createMUEvent(GAUGE_METRIC_NAME + "_max", 6, "jack=is-a", "dull=boy");
        MetricUpdateEvent gaugeMinEvent = createMUEvent(GAUGE_METRIC_NAME + "_min", 3, "jack=is-a", "dull=boy");
        MetricUpdateEvent gaugeMavgEvent = createMUEvent(GAUGE_METRIC_NAME + "_mean", 4.5, "jack=is-a", "dull=boy");

        DataflowAssert.that(fixedWindowResults).containsInAnyOrder(counterUpdateEvent, gaugeMaxEvent, gaugeMinEvent);
        DataflowAssert.that(slidingWindowEvents).containsInAnyOrder(gaugeMavgEvent);

        p.run();
    }

    private MetricUpdateEvent createMUEvent(String name, double value, String... kvStrings) {
        LinkedList<LabelNameValuePair> nvPairs = new LinkedList<>();

        for (String kv : kvStrings) {
            String[] items = kv.split("=");
            if (items.length != 2) {
                throw new RuntimeException("Bad key-value pair: " + kv);
            }

            nvPairs.addLast(new LabelNameValuePair(items[0], items[1]));
        }

        nvPairs.sort(LabelNameValuePair::compareTo);
        return new MetricUpdateEvent(name, nvPairs, value);
    }

    private void dumpMetric(MetricUpdateEvent event) {
        LOG.info("Metric {} => {} ... ", event.getName(), event.getValue());
        LOG.info("  Labels:");
        event.getLabelNameValuePairs().forEach(nvPair -> {
            LOG.info("    {} -> {}", nvPair.getName(), nvPair.getValue());
        });
    }
}