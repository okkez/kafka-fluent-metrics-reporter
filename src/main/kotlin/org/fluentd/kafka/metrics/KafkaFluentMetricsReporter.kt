package org.fluentd.kafka.metrics

import com.yammer.metrics.Metrics
import kafka.metrics.KafkaMetricsConfig
import kafka.metrics.KafkaMetricsReporter
import kafka.utils.VerifiableProperties
import org.komamitsu.fluency.Fluency
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class KafkaFluentMetricsReporter: KafkaMetricsReporter, KafkaFluentMetricsReporterMBean {

    companion object {
        val log = LoggerFactory.getLogger(this::class.java.enclosingClass)!!
    }

    private var enabled = false
    private var initialized = false
    private var running = false

    private lateinit var processor: FluentProcessor

    private var host = "localhost"
    private var port = 24224
    private var tagPrefix = "kafka-metrics"

    override fun init(props: VerifiableProperties?) {
        synchronized(this) {
            if (initialized) {
                return
            }
            val config = KafkaMetricsConfig(props)
            if (props != null) {
                enabled = props.getBoolean("kafka.fluent.metrics.enabled")
                host = props.getString("kafka.fluent.metrics.host")
                port = props.getInt("kafka.fluent.metrics.port")
                tagPrefix = props.getString("kafka.fluent.metrics.tagPrefix")
            }
            if (enabled) {
                log.info("Initialize FluentProcessor {}:{}, tagPrefix={}", host, port, tagPrefix)
                val fluencyConfig = Fluency.Config()
                processor = FluentProcessor(Metrics.defaultRegistry(), tagPrefix, host, port, fluencyConfig)
                startReporter(config.pollingIntervalSecs().toLong())
                initialized = true
            }
        }
    }

    override fun startReporter(pollingPeriodInSeconds: Long) {
        synchronized(this) {
            if (!enabled) {
                return
            }
            if (!running) {
                processor.start(pollingPeriodInSeconds, TimeUnit.SECONDS)
                running = true
                log.info("Started Kafka Fluent metrics reporter with polling period {} seconds",
                        pollingPeriodInSeconds)
            }
        }
    }

    override fun stopReporter() {
        synchronized(this) {
            if (initialized && running) {
                log.info("Stop Kafka Fluent metrics reporter")
                running = false
                processor.shutdown()
            }
        }
    }

    override fun getMBeanName(): String {
        return "kafka:type=${javaClass.canonicalName}"
    }
}