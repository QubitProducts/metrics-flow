package com.qubit.metricsflow.core.fn;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class VerifyMetricKeyTest {
    private static DoFnTester<KV<MetricUpdateKey, MetricUpdateValue>,
        KV<MetricUpdateKey, MetricUpdateValue>> fnTester = DoFnTester.of(new VerifyMetricKey());

    @Test
    public void metricName_containingOnlyAllowedSymbols_shouldPass() throws Exception {
        MetricUpdateKey key = MetricUpdateKey.of("mEtr_ic:Nam3", Collections.emptyList());
        MetricUpdateValue val = mock(MetricUpdateValue.class);

        fnTester.processElement(KV.of(key, val));
        List<KV<MetricUpdateKey, MetricUpdateValue>> output = fnTester.takeOutputElements();
        assertThat(output.size(), is(1));

        MetricUpdateKey outKey = output.get(0).getKey();
        assertThat(outKey, is(key));
    }

    @Test
    public void metricName_containingForbiddenSymbols_shouldNotPass() throws Exception {
        MetricUpdateKey key = MetricUpdateKey.of("mEtr_ic:Nam3-", Collections.emptyList());
        MetricUpdateValue val = mock(MetricUpdateValue.class);

        fnTester.processElement(KV.of(key, val));
        List<KV<MetricUpdateKey, MetricUpdateValue>> output = fnTester.takeOutputElements();
        assertThat(output.isEmpty(), is(true));
    }

    @Test
    public void metricName_canNot_startWithNumber() throws Exception {
        MetricUpdateKey key = MetricUpdateKey.of("001_metric_name", Collections.emptyList());
        MetricUpdateValue val = mock(MetricUpdateValue.class);

        fnTester.processElement(KV.of(key, val));
        List<KV<MetricUpdateKey, MetricUpdateValue>> output = fnTester.takeOutputElements();
        assertThat(output.isEmpty(), is(true));
    }

    @Test
    public void metricLabelName_containingOnlyAllowedSymbols_shouldPass() throws Exception {
        MetricUpdateKey key = MetricUpdateKey
            .of("xxx", Collections.singletonList(new LabelNameValuePair("la_b13_Nam3000", "value")));
        MetricUpdateValue val = mock(MetricUpdateValue.class);

        fnTester.processElement(KV.of(key, val));
        List<KV<MetricUpdateKey, MetricUpdateValue>> output = fnTester.takeOutputElements();
        assertThat(output.size(), is(1));

        MetricUpdateKey outKey = output.get(0).getKey();
        assertThat(outKey, is(key));
    }

    @Test
    public void metricLabelName_containingForbiddenSymbols_shouldNotPass() throws Exception {
        MetricUpdateKey key = MetricUpdateKey
            .of("xxx", Collections.singletonList(new LabelNameValuePair("la_-b13_Nam3000", "value")));
        MetricUpdateValue val = mock(MetricUpdateValue.class);

        fnTester.processElement(KV.of(key, val));
        List<KV<MetricUpdateKey, MetricUpdateValue>> output = fnTester.takeOutputElements();
        assertThat(output.isEmpty(), is(true));
    }

    @Test
    public void metricLabelName_canNot_startWithNumber() throws Exception {
        MetricUpdateKey key = MetricUpdateKey
            .of("xxx", Collections.singletonList(new LabelNameValuePair("123xyz", "value")));
        MetricUpdateValue val = mock(MetricUpdateValue.class);

        fnTester.processElement(KV.of(key, val));
        List<KV<MetricUpdateKey, MetricUpdateValue>> output = fnTester.takeOutputElements();
        assertThat(output.isEmpty(), is(true));
    }
}