package com.qubit.metricsflow.core;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface MetricsFlowOptions extends DataflowPipelineOptions {
    @Validation.Required
    @Description("Fixed window duration in seconds")
    @Default.Integer(10)
    Integer getFixedWindowDurationSec();
    void setFixedWindowDurationSec(Integer seconds);

    @Validation.Required
    @Description("Fixed window allowed lateness in seconds")
    @Default.Integer(0)
    Integer getFixedWindowAllowedLatenessSec();
    void setFixedWindowAllowedLatenessSec(Integer seconds);

    @Validation.Required
    @Description("Sliding window duration in seconds")
    @Default.Integer(5)
    Integer getSlidingWindowDurationSec();
    void setSlidingWindowDurationSec(Integer seconds);

    @Validation.Required
    @Description("Sliding window period in seconds")
    @Default.Integer(10)
    Integer getSlidingWindowPeriodSec();
    void setSlidingWindowPeriodSec(Integer seconds);

    @Validation.Required
    @Description("Output resource name (pubsub://<topic>, gs://<bucket>/<path-to-file>, log)")
    String getMetricsOutputResourceName();
    void setMetricsOutputResourceName(String resourceName);

    @Validation.Required
    @Default.Boolean(false)
    Boolean getIncludeProjectNameLabel();
    void setIncludeProjectNameLabel(Boolean includeProjectNameLabel);

    @Validation.Required
    @Default.Boolean(false)
    Boolean getIncludeJobNameLabel();
    void setIncludeJobNameLabel(Boolean includeJobNameLabel);

    @Validation.Required
    @Default.Boolean(true)
    Boolean getMetricsEnabled();
    void setMetricsEnabled(Boolean metricsEnabled);
}
