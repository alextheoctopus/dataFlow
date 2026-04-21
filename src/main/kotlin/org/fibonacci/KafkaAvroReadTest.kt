package org.fibonacci

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.common.serialization.ByteArrayDeserializer

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    env.enableCheckpointing(5000)

    val schema = Schema.Parser().parse(
        """
        {
          "type": "record",
          "name": "EcommerceRecord",
          "namespace": "org.fibonacci",
          "fields": [
            {"name":"InvoiceNo","type":["null","string"],"default":null},
            {"name":"StockCode","type":["null","string"],"default":null},
            {"name":"Description","type":["null","string"],"default":null},
            {"name":"Quantity","type":["null","int"],"default":null},
            {"name":"InvoiceDate","type":["null","long"],"default":null},
            {"name":"UnitPrice","type":["null","double"],"default":null},
            {"name":"CustomerID","type":["null","string"],"default":null},
            {"name":"Country","type":["null","string"],"default":null}
          ]
        }
        """.trimIndent()
    )

    val source = KafkaSource.builder<ByteArray>()
        .setBootstrapServers("kafka:9092")
        .setTopics("ecommerce-avro-topic")
        .setGroupId("flink-avro-read-test")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(
            KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer::class.java)
        )
        .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
        .map { bytes -> decodeAvro(bytes, schema) }
        .map { record ->
            "invoice=${record["InvoiceNo"]}, stock=${record["StockCode"]}, qty=${record["Quantity"]}, customer=${record["CustomerID"]}, country=${record["Country"]}"
        }
        .name("avro-debug-print")
        .print()

    env.execute("Kafka avro read test")
}

private fun decodeAvro(bytes: ByteArray, schema: Schema): GenericRecord {
    val reader = GenericDatumReader<GenericRecord>(schema)
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    return reader.read(null, decoder)
}