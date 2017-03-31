# Google Dataflow custom metrics made easy

`metrics-flow` is a library that allows you to create, aggregate and collect custom monitoring metrics from your [Dataflow](https://cloud.google.com/dataflow/)
pipelines and, with help of a small daemon [mflowd](https://github.com/qubitdigital/mflowd), put them to [Prometheus](https://prometheus.io/), 
a time-series database by SoundCloud 

## Why the heck do I need another metrics library? 

Dataflow [DoFns](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/DoFn), the functions
you want to collect metrics from, are usually distributed across multiple machines (Dataflow workers), which makes it tricky to collect 
and aggregate all the metrics from the all different instances the DoFns are running on.

One way to approach this problem is to expose a prometheus endpoint from each Dataflow worker individually and do the aggregation on prometheus
side (by grouping by label or something) but
  1. it won't work with [autoscaling](https://cloud.google.com/dataflow/service/dataflow-service-desc#autoscaling) (when the number of dataflow workers change dynamically depending on the current load)
  2. you need to make sure Prometheus can reach each worker to scrape metrics (you mgiht need a custom worker image for that but not sure)

With `metrics-flow` you don't need any of these. Just plug the library, create a [pub/sub](https://cloud.google.com/pubsub/docs/) topic
for your metrics and launch `mflowd` polling the topic subscription and that's it. It'll work with or without `autoscaling` and
it won't loose your metrics when `mflowd` or Prometheus is down (thanks to Google pub/sub). The cool thing about the library is that it uses 
[window](https://cloud.google.com/dataflow/model/windowing) functions to aggregate
the metrics by the means of your dataflow pipeline itself. Once the metrics are aggregated they are sent to the pub/sub topic which is polled by the `mflowd` daemon that exposes all 
the metrics it has received to Prometheus.

## Shut up and show me the code!

Every time a metric is recorded from a DoFn, `metrics-flow` sends an event to a [side output](https://cloud.google.com/dataflow/model/par-do#emitting-to-side-outputs-in-your-dofn) that 
 is transparently added to the DoFn. All the metric events emitted from DoFns get forwarded to  
 `FixedWindow` (Min/Max/Avg/Sum/etc), or to `SlidingWindow` (moving average and the like) aggregation function or both (yep, you can do that). Below you can find 
 some code that gives an impression of how it all fits together:

```java
    private static class DoFnWithCounter extends DoFn<Event, ...> {
        // I want a counter metric with two labels: userGroup and comanyName
        private static Counter numVisits = Counter
            .build()
            .named("numVisitsPerUserPerCompany")
            .labels("userGroup", "companyName")
            .create();

        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            Event event = processContext.element();
            numStrings.record(processContext) // boom, send a counter increase event
                .withLabel("userGroup", event.getUserGroup())
                .withLabel("companyName", event.getCompanyName())
                .inc();
        }
    }
    
    private static class DoFnWithGauge extends DoFn<Event, ...> {
        // I want a gague metric to calculate an average latency of my DoFn (actually I want it to be a moving average),
        // as well as its max and min values (for the sake of example) and I want it to have one label: eventType
        private static Gauge expensiveFunctionPerf = Gauge
            .build()
            .named("someExpensiveFunctionExecTime")
            .labels("eventType")
            .calculateMovingAverage()
            .calculateMax()
            .calculateMin()
            .create();

        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            Event event = processContext.element();
            long ts = System.currentTimeMillis();
            someExpensiveFunction(event);
            expensiveFunctionPerf.record(processContext) // boom, that's it, emit the event
                .withLabel("eventType", event.getType().toString())
                .set(System.currentTimeMillis() - ts);
        }
    }

    ...
    // An abstraction that kind of "collects" all the metrics being sent from all the DoFns
    MetricsBox mbox = MetricsBox.of(pipeline);

    // And that's how the pipeline may look like
    pipeline.apply(new ReadDataFromSomewhere())
        .apply(ParDo.of(new DoFnWithoutCounter())) // a function that doesn't use metrics
        .apply(CollectMetrics.from(ParDo.of(new DoFnWithMetrics())).into(mbox)) // a function that does use metrics
        .apply(ParDo.of(new AnotherDoFnWithoutMetrics))
        .apply(CollectMetrics.from(ParDo.of(new DoFnWithGauge())).into(mbox))
        ...

    mbox.apply(new DumpMetricsToPubsub()); // that's it
    ...
```
