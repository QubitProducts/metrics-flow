package com.qubit.metricsflow.metrics;

import com.qubit.metricsflow.metrics.core.mdef.MetricDefinition;
import com.qubit.metricsflow.metrics.core.mdef.MetricDefinitionBuilderBase;
import com.qubit.metricsflow.metrics.core.mdef.MetricRecorderBase;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nonnull;

public class Gauge extends MetricDefinition<Gauge.GaugeMetricRecorder> {
    protected Gauge(@Nonnull String name,
                 @Nonnull EnumSet<MetricAggregationType> fixedWindowAggregations,
                 @Nonnull EnumSet<MetricAggregationType> slidingWindowAggregations,
                 @Nonnull Set<String> labelNames) {
        super(name, fixedWindowAggregations, slidingWindowAggregations, labelNames);
    }

    public static GaugeMetricBuilder build() {
        return new GaugeMetricBuilder();
    }

    @Override
    public GaugeMetricRecorder record(DoFn.ProcessContext processContext) {
        return new GaugeMetricRecorder(this, processContext);
    }

    public static class GaugeMetricBuilder extends MetricDefinitionBuilderBase<Gauge, GaugeMetricBuilder> {
        public GaugeMetricBuilder calculateMin() {
            doEnableFixedWindowAggregation(MetricAggregationType.Min);
            return this;
        }

        public GaugeMetricBuilder calculateMax() {
            doEnableFixedWindowAggregation(MetricAggregationType.Max);
            return this;
        }

        public GaugeMetricBuilder calculateAverage() {
            doEnableFixedWindowAggregation(MetricAggregationType.Mean);
            return this;
        }

        public GaugeMetricBuilder calculateMovingAverage() {
            doEnableSlidingWindowAggregation(MetricAggregationType.Mean);
            return this;
        }

        @Override
        public Gauge create() {
            throwExceptionIfParametersAreInvalid();
            return new Gauge(name, fixedWindowAggregations, slidingWindowAggregations, labels);
        }
    }

    public static class GaugeMetricRecorder extends MetricRecorderBase<GaugeMetricRecorder> {
        protected GaugeMetricRecorder(MetricDefinition<GaugeMetricRecorder> metricDefinition,
                                      DoFn.ProcessContext pctx) {
            super(metricDefinition, pctx);
        }

        public void set(double value) {
            super.doPush(value);
        }
    }
}
