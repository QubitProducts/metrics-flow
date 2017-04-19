package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.MetricsFlowOptions;
import com.qubit.metricsflow.core.utils.ImplicitLabelNames;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import java.util.List;

public class IncludeExtraLabels extends PTransform<PCollection<MetricUpdateEvent>, PCollection<MetricUpdateEvent>> {
    public IncludeExtraLabels() {
        super("IncludeExtraLabels");
    }

    @Override
    public PCollection<MetricUpdateEvent> apply(PCollection<MetricUpdateEvent> input) {
        MetricsFlowOptions options = input.getPipeline().getOptions().as(MetricsFlowOptions.class);
        String projectName = options.getIncludeProjectNameLabel() ? options.getProject() : null;
        String jobName = options.getIncludeJobNameLabel() ? options.getJobName() : null;
        if (projectName == null && jobName == null) {
            return input;
        }

        return input.apply(
            MapElements.via((MetricUpdateEvent event) -> {
                MetricUpdateEvent newEvent = new MetricUpdateEvent(event);
                List<LabelNameValuePair> nvPairs = newEvent.getLabelNameValuePairs();
                if (projectName != null) {
                    nvPairs.add(
                        new LabelNameValuePair(ImplicitLabelNames.GCP_PROJECT_NAME, projectName)
                    );
                }
                if (jobName != null) {
                    nvPairs.add(
                        new LabelNameValuePair(ImplicitLabelNames.GCP_JOB_NAME, jobName)
                    );
                }

                nvPairs.sort(LabelNameValuePair::compareTo);
                return newEvent;
            }).withOutputType(new TypeDescriptor<MetricUpdateEvent>() {})
        );
    }
}
