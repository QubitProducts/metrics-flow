package com.qubit.metricsflow.core;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;

public interface MetricsFlowOptions extends DataflowPipelineOptions {
    @Validation.Required
    @Description("Fixed window duration in seconds")
    @Default.Integer(15)
    Integer getFixedWindowDurationSec();
    void setFixedWindowDurationSec(Integer seconds);

    @Validation.Required
    @Description("Fixed window allowed lateness in seconds")
    @Default.Integer(0)
    Integer getFixedWindowAllowedLatenessSec();
    void setFixedWindowAllowedLatenessSec(Integer seconds);

    @Validation.Required
    @Description("Sliding window duration in seconds")
    @Default.Integer(60)
    Integer getSlidingWindowDurationSec();
    void setSlidingWindowDurationSec(Integer seconds);

    @Validation.Required
    @Description("Sliding window period in seconds")
    @Default.Integer(15)
    Integer getSlidingWindowPeriodSec();
    void setSlidingWindowPeriodSec(Integer seconds);

    @Validation.Required
    @Description("Output topic name")
    String getOutputTopicName();
    void setOutputTopicName(String topicName);
}
