# kafka-fluent-metrics-reporter

Kafka metrics reporter for Fluentd

## Build

```
$ ./gradlew shadowJar
```

## Install

Copy fat jar to your class path:

```
$ cp build/libs/kafka-fluent-metrics-reporter-1.0-SNAPSHOT-all.jar /path/to/kafka_2.11-1.0.0/libs
```

## Configuration

Add following lines to your Kafka server's server.properties:

```
kafka.metrics.reporters=org.fluentd.kafka.metrics.KafkaFluentMetricsReporter
kafka.metrics.polling.interval.secs=5
kafka.fluent.metrics.enabled=true
kafka.fluent.metrics.host=localhost
kafka.fluent.metrics.port=24225
kafka.fluent.metrics.tagPrefix=kafka-metrics
```

And run your Kafka server.

