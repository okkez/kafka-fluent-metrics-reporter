package org.fluentd.kafka.metrics

import com.yammer.metrics.core.*
import com.yammer.metrics.reporting.AbstractPollingReporter
import org.komamitsu.fluency.Fluency

class FluentProcessor(registry: MetricsRegistry?,
                      private val tagPrefix: String,
                      host: String,
                      port: Int,
                      fluencyConfig: Fluency.Config)
    : AbstractPollingReporter(registry, "kafka-fluent-metrics-reporter"), MetricProcessor<Long> {

    private val fluency = Fluency.defaultFluency(host, port, fluencyConfig)

    override fun run() {
        processRegularMetrics(System.currentTimeMillis() / 1000)
    }

    override fun shutdown() {
        fluency.close()
        super.shutdown()
    }

    private fun processRegularMetrics(timestamp: Long) {
        metricsRegistry.groupedMetrics(MetricPredicate.ALL).entries.forEach { entry ->
            entry.value.entries.forEach { subEntry ->
                subEntry.value.processWith(this, subEntry.key, timestamp)
            }
        }
    }

    override fun processCounter(name: MetricName?, counter: Counter?, timestamp: Long?) {
        if (counter != null) {
            val tag = "$tagPrefix.counter"
            val map = mutableMapOf<String, Any>()
            map["Counter"] = counter.count()
            if (timestamp != null) {
                fluency.emit(tag, timestamp, map)
            } else {
                fluency.emit(tag, map)
            }
        }
    }

    override fun processMeter(name: MetricName?, meter: Metered?, timestamp: Long?) {
        if (meter != null) {
            val tag = "$tagPrefix.meter"
            val map = mutableMapOf<String, Any>()
            map["Counter"] = meter.count()
            map["MeanRate"] = meter.meanRate()
            map["FifteenMinuteRate"] = meter.fifteenMinuteRate()
            map["FiveMinuteRate"] = meter.fiveMinuteRate()
            map["OneMinuteRate"] = meter.oneMinuteRate()
            if (timestamp != null) {
                fluency.emit(tag, timestamp, map)
            } else {
                fluency.emit(tag, map)
            }
        }
    }

    override fun processHistogram(name: MetricName?, histogram: Histogram?, timestamp: Long?) {
        if (histogram != null) {
            val tag = "$tagPrefix.histogram"
            val map = mutableMapOf<String, Any>()
            map["Count"] = histogram.count()
            map["Max"] = histogram.max()
            map["Mean"] = histogram.mean()
            map["Min"] = histogram.min()
            map["StdDev"] = histogram.stdDev()
            map["Sum"] = histogram.sum()
            val snapshot = histogram.snapshot
            map["95thPercentile"] = snapshot.get95thPercentile()
            map["98thPercentile"] = snapshot.get98thPercentile()
            map["99thPercentile"] = snapshot.get99thPercentile()
            map["999thPercentile"] = snapshot.get999thPercentile()
            if (timestamp != null) {
                fluency.emit(tag, timestamp, map)
            } else {
                fluency.emit(tag, map)
            }
        }
    }

    override fun processTimer(name: MetricName?, timer: Timer?, timestamp: Long?) {
        if (timer != null) {
            val tag = "$tagPrefix.timer"
            val map = mutableMapOf<String, Any>()
            map["Count"] = timer.count()
            map["MeanRate"] = timer.meanRate()
            map["FifteenMinuteRate"] = timer.fifteenMinuteRate()
            map["FiveMinuteRate"] = timer.fiveMinuteRate()
            map["OneMinuteRate"] = timer.oneMinuteRate()
            map["Max"] = timer.max()
            map["Mean"] = timer.mean()
            map["Min"] = timer.min()
            map["StdDev"] = timer.stdDev()
            map["Sum"] = timer.sum()
            if (timestamp != null) {
                fluency.emit(tag, timestamp, map)
            } else {
                fluency.emit(tag, map)
            }
        }
    }

    override fun processGauge(name: MetricName?, gauge: Gauge<*>?, timestamp: Long?) {
        if (gauge != null) {
            val tag = "$tagPrefix.gauge"
            val map = mutableMapOf<String, Any>()
            map["Value"] = gauge.value()
            if (timestamp != null) {
                fluency.emit(tag, timestamp, map)
            } else {
                fluency.emit(tag, map)
            }
        }
    }
}