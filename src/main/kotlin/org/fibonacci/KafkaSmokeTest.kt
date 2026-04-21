package org.fibonacci

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.common.serialization.ByteArrayDeserializer
fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val source = KafkaSource.builder<ByteArray>()
        .setBootstrapServers("kafka:9092")
        .setTopics("ecommerce-avro-topic")
        .setGroupId("flink-smoke-test")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(
            KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer::class.java)
        )
        .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
        .map { bytes -> "len=${bytes.size}" }
        .print()

    env.execute("Kafka smoke test")
}