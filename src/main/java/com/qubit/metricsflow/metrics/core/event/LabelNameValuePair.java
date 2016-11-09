package com.qubit.metricsflow.metrics.core.event;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import java.io.Serializable;

import javax.annotation.Nonnull;

@DefaultCoder(AvroCoder.class)
public final class LabelNameValuePair implements Serializable, Comparable<LabelNameValuePair> {
    private final String name;
    private final String value;

    // For AvroCoder
    private LabelNameValuePair() {
        name = "";
        value = "";
    }

    public LabelNameValuePair(@Nonnull String labelName, @Nonnull String labelValue) {
        this.name = labelName;
        this.value = labelValue;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof LabelNameValuePair)) {
            return false;
        }
        if (o == this) {
            return true;
        }

        LabelNameValuePair that = (LabelNameValuePair)o;
        return this.name.equals(that.name) &&
               this.value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return 31 * name.hashCode() + value.hashCode();
    }

    @Override
    public int compareTo(@Nonnull LabelNameValuePair that) {
        return this.name.compareTo(that.name);
    }
}
