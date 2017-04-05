package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import javaslang.control.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public class VerifyMetricKey extends DoFn<KV<MetricUpdateKey, MetricUpdateValue>, KV<MetricUpdateKey, MetricUpdateValue>> {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyMetricKey.class);

    // these two were copy-pasted as is from io.prometheus.client.Collector
    private static final Pattern METRIC_NAME_RE = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
    private static final Pattern METRIC_LABEL_NAME_RE = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    private final Aggregator<Long, Long> verificationPasses =
        createAggregator("VerificationPasses", new Sum.SumLongFn());
    private final Aggregator<Long, Long> verificationFailures =
        createAggregator("VerificationFailures", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        KV<MetricUpdateKey, MetricUpdateValue> event = processContext.element();
        Either<String, MetricUpdateKey> verificationResult = verifyMetricName(event.getKey())
            .flatMap(VerifyMetricKey::verifyMetricLabels);
        if (verificationResult.isRight()) {
            processContext.output(event);
            verificationPasses.addValue(1L);
        } else {
            LOG.error("Error: {}", verificationResult.getLeft());
            verificationFailures.addValue(1L);
        }
    }

    private static Either<String, MetricUpdateKey> verifyMetricName(MetricUpdateKey metricUpdateKey) {
        String metricName = metricUpdateKey.getMetricName();
        if (!METRIC_NAME_RE.matcher(metricName).matches()) {
            return Either.left(String.format("Metric name \"%s\" contains forbidden symbols", metricName));
        }

        return Either.right(metricUpdateKey);
    }

    private static Either<String, MetricUpdateKey> verifyMetricLabels(MetricUpdateKey metricUpdateKey) {
        for (LabelNameValuePair nvp : metricUpdateKey.getLabelNameValuePairs()) {
            Either<String, MetricUpdateKey> res = verifyLabelName(metricUpdateKey, nvp);
            if (res.isLeft()) {
                return res;
            }
        }

        return Either.right(metricUpdateKey);
    }

    private static Either<String, MetricUpdateKey> verifyLabelName(MetricUpdateKey metricUpdateKey,
                                                                   LabelNameValuePair labelNameValue) {
        if (!METRIC_LABEL_NAME_RE.matcher(labelNameValue.getName()).matches()) {
            return Either.left(String.format("Bad label name: \"%s\"", labelNameValue.getName()));
        }

        return Either.right(metricUpdateKey);
    }
}
