package com.qubit.metricsflow.core.fn;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.qubit.metricsflow.core.TestUtils;
import com.qubit.metricsflow.core.types.MetricUpdateKey;
import com.qubit.metricsflow.core.types.MetricUpdateValue;
import com.qubit.metricsflow.core.utils.WindowTypeTags;
import com.qubit.metricsflow.metrics.core.types.MetricAggregationType;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

public class BranchByWindowTypeTest {
    private static DoFnTester<KV<MetricUpdateKey, MetricUpdateValue>, Void> fnTester =
        DoFnTester.of(new BranchByWindowType());

    @Test
    public void fixedWindowMetrics_shouldGoTo_fixedWindowSideOutput() throws Exception {
        MetricUpdateValue val = mock(MetricUpdateValue.class);
        when(val.getFixedWindowAggregations()).thenReturn(EnumSet.of(MetricAggregationType.Max));
        when(val.getSlidingWindowAggregations()).thenReturn(EnumSet.noneOf(MetricAggregationType.class));

        KV<MetricUpdateKey, MetricUpdateValue> input = KV.of(mock(MetricUpdateKey.class), val);
        List<Void> shouldBeEmpty = fnTester.processBundle(Collections.singletonList(input));
        TestUtils.assertThatListIsEmpty(shouldBeEmpty);

        List<KV<MetricUpdateKey, MetricUpdateValue>> fixedOutput =
            fnTester.takeOutputElements(WindowTypeTags.FIXED_IN);
        TestUtils.assertThatListIsNotEmpty(fixedOutput);
        assertThat(fixedOutput.get(0), is(input));

        List<KV<MetricUpdateKey, MetricUpdateValue>> slidingOutput =
            fnTester.takeOutputElements(WindowTypeTags.SLIDING_IN);
        TestUtils.assertThatListIsEmpty(slidingOutput);
    }

    @Test
    public void slidingWindowMetrics_shouldGoTo_slidingWindowSideOutput() throws Exception {
        MetricUpdateValue val = mock(MetricUpdateValue.class);
        when(val.getSlidingWindowAggregations()).thenReturn(EnumSet.of(MetricAggregationType.Mean));
        when(val.getFixedWindowAggregations()).thenReturn(EnumSet.noneOf(MetricAggregationType.class));

        KV<MetricUpdateKey, MetricUpdateValue> input = KV.of(mock(MetricUpdateKey.class), val);
        List<Void> shouldBeEmpty = fnTester.processBundle(Collections.singletonList(input));
        TestUtils.assertThatListIsEmpty(shouldBeEmpty);

        List<KV<MetricUpdateKey, MetricUpdateValue>> slidingOutput =
            fnTester.takeOutputElements(WindowTypeTags.SLIDING_IN);
        TestUtils.assertThatListIsNotEmpty(slidingOutput);
        assertThat(slidingOutput.get(0), is(input));

        List<KV<MetricUpdateKey, MetricUpdateValue>> fixedOutput =
            fnTester.takeOutputElements(WindowTypeTags.FIXED_IN);
        TestUtils.assertThatListIsEmpty(fixedOutput);
    }

    @Test
    public void ifBoth_FixedAndSlidingMetricsAreUsed_bothSideOutputs_shouldNotBeEmpty() throws Exception {
        MetricUpdateValue val = mock(MetricUpdateValue.class);
        when(val.getFixedWindowAggregations()).thenReturn(EnumSet.of(MetricAggregationType.Mean));
        when(val.getSlidingWindowAggregations()).thenReturn(EnumSet.of(MetricAggregationType.Mean));

        KV<MetricUpdateKey, MetricUpdateValue> input = KV.of(mock(MetricUpdateKey.class), val);
        List<Void> shouldBeEmpty = fnTester.processBundle(Collections.singletonList(input));
        TestUtils.assertThatListIsEmpty(shouldBeEmpty);

        List<KV<MetricUpdateKey, MetricUpdateValue>> fixedOutput =
            fnTester.takeOutputElements(WindowTypeTags.FIXED_IN);
        TestUtils.assertThatListIsNotEmpty(fixedOutput);
        assertThat(fixedOutput.get(0), is(input));

        List<KV<MetricUpdateKey, MetricUpdateValue>> slidingOutput =
            fnTester.takeOutputElements(WindowTypeTags.SLIDING_IN);
        TestUtils.assertThatListIsNotEmpty(slidingOutput);
        assertThat(slidingOutput.get(0), is(input));
    }

}