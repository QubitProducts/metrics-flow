package com.qubit.metricsflow.metrics.core.mdef;

import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

import java.util.EnumSet;
import java.util.Set;

public class RawMetricDefinition extends MetricDefinition<RawMetricDefinition.RawMetricRecorder> {
    protected RawMetricDefinition(String name,
                                  EnumSet<MetricAggregationType> fixedWindowAggregations,
                                  EnumSet<MetricAggregationType> slidingWindowAggregations,
                                  Set<String> labelNames) {
        super(name, fixedWindowAggregations, slidingWindowAggregations, labelNames);
    }

    public static RawMetricBuilder build() {
        return new RawMetricBuilder();
    }

    @Override
    public RawMetricRecorder record(DoFn.ProcessContext processContext) {
        return new RawMetricRecorder(getName(), processContext);
    }

    public static class RawMetricBuilder extends MetricDefinitionBuilderBase<RawMetricDefinition, RawMetricBuilder> {
        protected RawMetricBuilder() {
        }

        public RawMetricBuilder enableFixedWindowAggregation(MetricAggregationType aggregation) {
            return doEnableFixedWindowAggregation(aggregation);
        }

        public RawMetricBuilder enableSlidingWindowAggregation(MetricAggregationType aggregation) {
            return doEnableSlidingWindowAggregation(aggregation);
        }

        @Override
        protected RawMetricDefinition createDefinition() {
            throwExceptionIfParametersAreInvalid();
            return new RawMetricDefinition(name, fixedWindowAggregations, slidingWindowAggregations, labels);
        }
    }

    public static class RawMetricRecorder extends MetricRecorderBase<RawMetricRecorder> {
        protected RawMetricRecorder(String metricName, DoFn.ProcessContext pctx) {
            super(metricName, pctx);
        }

        public void push(double value) {
            super.doPush(value);
        }
    }
}