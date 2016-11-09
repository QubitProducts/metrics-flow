package com.qubit.metricsflow.metrics;

import com.qubit.metricsflow.metrics.core.mdef.MetricDefinition;
import com.qubit.metricsflow.metrics.core.mdef.MetricDefinitionBuilderBase;
import com.qubit.metricsflow.metrics.core.mdef.MetricRecorderBase;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

import java.util.EnumSet;
import java.util.Set;

public class Counter extends MetricDefinition<Counter.CounterMetricRecorder> {
    protected Counter(String name,
                      EnumSet<MetricAggregationType> fixedWindowAggregations,
                      EnumSet<MetricAggregationType> slidingWindowAggregations,
                      Set<String> labelNames) {
        super(name, fixedWindowAggregations, slidingWindowAggregations, labelNames);
    }

    public static CounterMetricBuilder build() {
        return new CounterMetricBuilder();
    }

    @Override
    public CounterMetricRecorder record(DoFn.ProcessContext processContext) {
        return new CounterMetricRecorder(getName(), processContext);
    }

    public static class CounterMetricBuilder extends MetricDefinitionBuilderBase<Counter, CounterMetricBuilder> {
        protected CounterMetricBuilder() {
            doEnableFixedWindowAggregation(MetricAggregationType.Sum);
        }

        @Override
        protected Counter createDefinition() {
            throwExceptionIfParametersAreInvalid();
            return new Counter(name, fixedWindowAggregations, slidingWindowAggregations, labels);
        }
    }

    public static class CounterMetricRecorder extends MetricRecorderBase<CounterMetricRecorder> {
        protected CounterMetricRecorder(String metricName, DoFn.ProcessContext pctx) {
            super(metricName, pctx);
        }

        public void inc() {
            super.doPush(1.0);
        }

        public void dec() {
            super.doPush(-1.0);
        }
    }
}
