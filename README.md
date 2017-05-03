# Google Dataflow custom metrics made easy

`metrics-flow` is a library that allows to create, aggregate and collect custom monitoring metrics from [Dataflow](https://cloud.google.com/dataflow/)
pipelines and, with help of a small daemon [mflowd](https://github.com/qubitdigital/mflowd), put them to [Prometheus](https://prometheus.io/), 
a time-series database by SoundCloud.

## Why the heck do I need another metrics library? 

Dataflow [DoFns](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/DoFn), the functions
you want to collect metrics from, are usually distributed across multiple machines (Dataflow workers), which makes it tricky to collect 
and aggregate all the metrics from all the different instances a DoFn is running on.

One way to approach this problem is to expose a prometheus endpoint from each Dataflow worker individually and do the aggregation on Prometheus
side (by grouping metrics by label or something like that) but
  1. it won't work with [autoscaling](https://cloud.google.com/dataflow/service/dataflow-service-desc#autoscaling) (when the number of dataflow workers change dynamically depending on the current load) as the addresses of `scraping targets` are statically set up in Prometheus config. 
  2. you need to make sure Prometheus can reach each worker to scrape the metrics (you mgiht need a custom worker image for that but not sure)

Another way is to use [java-statsd-client](https://github.com/tim-group/java-statsd-client) with [statsd_exporter](https://github.com/prometheus/statsd_exporter) but there's one probelem: the library sends a UDP packet _every time a metric is updated_, which may lead to significant overhead once the pipeline starts generating tens of thouthands of metrics.

With `metrics-flow` you don't need any of these. Just plug the library, create a [pub/sub](https://cloud.google.com/pubsub/docs/) topic
for your metrics and launch `mflowd` polling the topic subscription and that's it. It'll work with or without `autoscaling`,
it won't loose your metrics when `mflowd` or Prometheus is down (thanks to Google pub/sub) and finally it will pre-aggregate metrics locally (using [window](https://cloud.google.com/dataflow/model/windowing) functions) before writing them to the queue which significantly reduces the number of emited metric update events. Once the metrics are pre-aggregated they are sent to the pub/sub topic which is polled by the `mflowd` daemon that exposes all the metrics it has received to Prometheus.

# So what exactly can it do?

The library is designed to look and feel like [prometheus java client](https://github.com/prometheus/client_java) but there're some minor differences due to the nature of Google Dataflow. The basics are all the same. There're different types of metrics, each metric has a unique name and may have a set of labels (or dimentions) associated with it. Supported metric types:

* Counter
* Gauge

Below you can find more information about each of them.

## Counter

Counters are strictly increasing values, they can only go up.

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
           ...
           numVisits.record(processContext)
              .withLabel("userGroup", event.getUserGroup())
              .withLabel("companyName", event.getCompanyName())
              .inc();
        }
    }
```

## Gauge

Unlike counters, gauge metrics don't have any restrictions on the way they can chage. Any floating point value can be assigned to a gauge.
Unlike classic gauges that you might have met in the prometheus java client library, the ones from `metrics-flow` are slightly more complex as they allow user to select a type of aggregation function (or functions) that will be applied to the recorded values.

Gauge metric supports four different types of aggregations. Almost all of them can be turned on together:
* Max
* Min
* Average (can not be used with moving average)
* Moving average (can not be used with average)

```java
    private static class DoFnWithGauge extends DoFn<Event, ...> {
        // I want a gague metric to calculate an average latency of my DoFn 
        // (actually I want it to be a moving average),
        // as well as its max and min values (for the sake of example) and 
        // I want it to have one label: eventType
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
```

Aggregation function type is automatically appended to the name of your metric before it is sent to `mflowd`. So that `eventType` from the code above will result in three different metrics in Prometheus: *eventType_mean*, *eventType_max* and *eventType_min*.

## Configuration options

Once you inherit your pipeline options class from `MetricsFlowOptions` you get a bunch of configuration parameters to tweak:

**Basic options**:
* *metricsOutputResourceName*: controls where to send pre-aggregated metrics. There're three options available at the moment:
   * `pubsub://<topic-nmae>`: dump metrics to a pub/sub topic
   * `gs://<bucket-name>/<path-to-file>`: dump metrics to [Google Cloud Storage](https://cloud.google.com/storage/docs/cloud-console) (NOTE: works only in batch mode)
   * `log`: just write pre-aggregated metrics to your dataflow pipeline logs with `INFO` log level 
* *includeJobNameLabel:* if set to true metrics-flow automatically adds `gcpJobName` label containing current dataflow job name (default: false)
* *includeProjectNameLabel*: if set to true metrics-flow adds `gcpProjcetName` label that will include current dataflow project name (default: false)
* *metricsEnabled*: set to false to disable metrics-flow (default: true)

**Advanced options:**
* *fixedWindowDurationSec* the duration of fixed window used for pre-aggregation of counters, max, min and avg gagues (default=10).
* *fixedWindowAllowedLatenessSec* allowed lateness (default=0), see [this](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/windowing/Window.html#withAllowedLateness-org.joda.time.Duration-) for more information
* *slidingWindowDurationSec* sliding window duration (default=5)
* *slidingWindowPeriodSec* sliding window period (default=10s)

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
            numVisits.record(processContext) // boom, send a counter increase event
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

    mbox.run(); // that's it
    ...
```
