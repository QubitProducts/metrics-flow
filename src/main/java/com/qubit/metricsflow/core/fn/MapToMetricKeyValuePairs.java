package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.metrics.MetricsRegistry;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;
import com.qubit.metricsflow.metrics.core.mdef.MetricDefinition;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Sets;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import javaslang.control.Either;
import javaslang.control.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MapToMetricKeyValuePairs extends DoFn<MetricUpdateEvent, KV<MetricUpdateKey, Double>> {
    private static final Logger LOG = LoggerFactory.getLogger(MapToMetricKeyValuePairs.class);

    // these two were copy-pasted as is from io.prometheus.client.Collector
    private static final Pattern METRIC_NAME_RE = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
    private static final Pattern METRIC_LABEL_NAME_RE = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");


    private final Aggregator<Long, Long> verificationPasses = createAggregator("VerificationPasses", new Sum.SumLongFn());
    private final Aggregator<Long, Long> verificationFailures = createAggregator("VerificationFailures", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        MetricUpdateEvent event = processContext.element();
        Validation<String, KV<MetricUpdateKey, Double>> result = verifyMetricName(event)
            .flatMap(this::verifyMetricLabels)
            .flatMap(labelNvPairs -> mapToKeyValue(event, labelNvPairs));

        if (result.isValid()) {
            processContext.output(result.get());
            verificationPasses.addValue(1L);
        } else {
            LOG.error("Error: {}", result.getError());
            verificationFailures.addValue(1L);
        }
    }

    private Validation<String, MetricUpdateEvent> verifyMetricName(MetricUpdateEvent event) {
        if (!METRIC_NAME_RE.matcher(event.getName()).matches()) {
            return Validation.invalid(String.format("Metric name \"%s\" contains forbidden symbols", event.getName()));
        }

        return Validation.valid(event);
    }

    private Validation<String, List<LabelNameValuePair>> verifyMetricLabels(MetricUpdateEvent event) {
        Either<String, MetricDefinition> mbMdef = MetricsRegistry.getDefaultRegistry()
            .find(event.getName());
        if (mbMdef.isLeft()) {
            return Validation.invalid(mbMdef.getLeft());
        }

        MetricDefinition mdef = mbMdef.get();
        return verifyLabelsQuantity(mdef, event)
            .flatMap(nvPairs -> verifyLabelNames(mdef, nvPairs));
    }

    private Validation<String, List<LabelNameValuePair>> verifyLabelsQuantity(MetricDefinition<?> mdef,
                                                                              MetricUpdateEvent event) {
        List<LabelNameValuePair> nvPairs = event.getLabelNameValuePairs();
        Set<String> usedLabelNames = nvPairs.stream().map(LabelNameValuePair::getName).collect(Collectors.toSet());
        int numUnusedLabels = Sets.difference(mdef.getLabelNames(), usedLabelNames).size();
        if (numUnusedLabels > 0) {
            return Validation.invalid(String.format("There're %d labels not being used for metric \"%s\"",
                                                    numUnusedLabels, mdef.getName()));
        }

        return Validation.valid(nvPairs);
    }

    private Validation<String, List<LabelNameValuePair>> verifyLabelNames(MetricDefinition<?> mdef,
                                                                          List<LabelNameValuePair> nvPairs) {
        for (LabelNameValuePair nvPair : nvPairs) {
            Validation<String, LabelNameValuePair> result = verifyLabelName(mdef, nvPair);
            if (result.isInvalid()) {
                return Validation.invalid(result.getError());
            }
        }

        return Validation.valid(new LinkedList<>(nvPairs));
    }

    private Validation<String, KV<MetricUpdateKey, Double>> mapToKeyValue(
        MetricUpdateEvent event, List<LabelNameValuePair> labelsNvPairs) {
        MetricUpdateKey key = MetricUpdateKey.of(event.getName(), labelsNvPairs);

        return Validation.valid(KV.of(key, event.getValue()));
    }

    private Validation<String, LabelNameValuePair> verifyLabelName(MetricDefinition<?> mdef,
                                                                   LabelNameValuePair labelNameValue) {
        if (!METRIC_LABEL_NAME_RE.matcher(labelNameValue.getName()).matches()) {
            return Validation.invalid(String.format("Bad label name: \"%s\"", labelNameValue.getName()));
        }
        if (!mdef.getLabelNames().contains(labelNameValue.getName())) {
            return Validation.invalid(String.format("Label name \"%s\" is not registered in the metric \"%s\"",
                                                    labelNameValue.getName(), mdef.getName()));
        }

        return Validation.valid(labelNameValue);
    }
}
