package com.qubit.metricsflow.core.fn;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

public class VerifyMetricKey extends DoFn<KV<MetricUpdateKey, MetricUpdateValue>, KV<MetricUpdateKey, MetricUpdateValue>> {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyMetricKey.class);

    // these two were copy-pasted as is from io.prometheus.client.Collector
    private static final Pattern METRIC_NAME_RE = Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*");
    private static final Pattern METRIC_LABEL_NAME_RE = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");


    private final Aggregator<Long, Long> verificationPasses = createAggregator("VerificationPasses", new Sum.SumLongFn());
    private final Aggregator<Long, Long> verificationFailures = createAggregator("VerificationFailures", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        KV<MetricUpdateKey, MetricUpdateValue> event = processContext.element();

        try {
            verifyMetricName(event.getKey().getMetricName());
            verifyMetricLabels(event.getKey().getLabelNameValuePairs());
            processContext.output(event);
            verificationPasses.addValue(1L);
        } catch (RuntimeException e) {
            LOG.error("Error: {}", e);
            verificationFailures.addValue(1L);
        }
    }

    private static void verifyMetricName(String metricName) {
        if (!METRIC_NAME_RE.matcher(metricName).matches()) {
            throw new RuntimeException(String.format("Metric name \"%s\" contains forbidden symbols", metricName));
        }
    }

    private static void verifyMetricLabels(List<LabelNameValuePair> labelNameValuePairs) {
        labelNameValuePairs.forEach(VerifyMetricKey::verifyLabelName);
    }

    private static void verifyLabelName(LabelNameValuePair labelNameValue) {
        if (!METRIC_LABEL_NAME_RE.matcher(labelNameValue.getName()).matches()) {
            throw new RuntimeException(String.format("Bad label name: \"%s\"", labelNameValue.getName()));
        }
    }
}
