package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;

import javaslang.control.Either;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public class VerifyMetricKey extends
                             DoFn<KV<MetricUpdateKey, MetricUpdateValue>, KV<MetricUpdateKey, MetricUpdateValue>> {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyMetricKey.class);

    // these two were copy-pasted as is from io.prometheus.client.Collector
    private static final Pattern METRIC_NAME_RE = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
    private static final Pattern METRIC_LABEL_NAME_RE = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    private final Counter verificationPasses = Metrics.counter(VerifyMetricKey.class, "VerificationPasses");
    private final Counter verificationFailures = Metrics.counter(VerifyMetricKey.class, "VerificationFailures");

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
        KV<MetricUpdateKey, MetricUpdateValue> event = processContext.element();
        Either<String, MetricUpdateKey> verificationResult = verifyMetricName(event.getKey())
            .flatMap(VerifyMetricKey::verifyMetricLabels);
        if (verificationResult.isRight()) {
            processContext.output(event);
            verificationPasses.inc(1L);
        } else {
            LOG.error("Error: {}", verificationResult.getLeft());
            verificationFailures.inc(1L);
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
