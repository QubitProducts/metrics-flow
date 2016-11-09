package com.qubit.metricsflow.metrics;

import com.qubit.metricsflow.metrics.core.mdef.MetricDefinition;

import javaslang.control.Either;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MetricsRegistry {
    private static MetricsRegistry defaultRegistry;
    protected ConcurrentHashMap<String, MetricDefinition> registeredMetrics = new ConcurrentHashMap<>();

    public static MetricsRegistry getDefaultRegistry() {
        if (defaultRegistry == null) {
            synchronized (MetricsRegistry.class) {
                if (defaultRegistry == null) {
                    defaultRegistry = new MetricsRegistry();
                }
            }
        }

        return defaultRegistry;
    }

    public void register(MetricDefinition mdef) {
        MetricDefinition prev = registeredMetrics.putIfAbsent(mdef.getName(), mdef);
        if (prev != null) {
            throw new RuntimeException(String.format("Metric with name \"%s\" has already been registered",
                                                     mdef.getName()));
        }
    }

    public Either<String, MetricDefinition> find(String metricName) {
        MetricDefinition mdef = registeredMetrics.get(metricName);
        if (mdef == null) {
            return Either.left(String.format("Metric definition with name \"%s\" not found", metricName));
        }

        return Either.right(mdef);
    }

    public MetricDefinition getOrThrowException(String metricName) {
        return this.find(metricName)
            .getOrElseThrow((Function<String, NoSuchElementException>) NoSuchElementException::new);
    }
}
