package com.qubit.metricsflow.core.transform;

import com.qubit.metricsflow.core.MetricsFlowOptions;
import com.qubit.metricsflow.core.utils.ImplicitLabelNames;
import com.qubit.metricsflow.metrics.core.event.LabelNameValuePair;
import com.qubit.metricsflow.metrics.core.event.MetricUpdateEvent;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.List;

public class IncludeExtraLabels extends PTransform<PCollection<MetricUpdateEvent>, PCollection<MetricUpdateEvent>> {
    private final String projectName;
    private final String jobName;

    public IncludeExtraLabels(String projectName, String jobName) {
        super("IncludeExtraLabels");
        this.projectName = projectName;
        this.jobName = jobName;
    }

    @Override
    public PCollection<MetricUpdateEvent> expand(PCollection<MetricUpdateEvent> input) {
        if (projectName == null && jobName == null) {
            return input;
        }

        return input.apply(
            MapElements.into(new TypeDescriptor<MetricUpdateEvent>() {
            }).via((MetricUpdateEvent event) -> {
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
            })
        );
    }
}
