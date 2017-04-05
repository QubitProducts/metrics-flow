package com.qubit.metricsflow.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.List;

public class TestUtils {
    public static void assertThatListIsEmpty(List<?> lst) {
        assertThat(lst.isEmpty(), is(true));
    }

    public static void assertThatListIsNotEmpty(List<?> lst) {
        assertThat(lst.isEmpty(), is(false));
    }
}
