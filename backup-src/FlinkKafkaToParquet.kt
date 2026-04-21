package org.fibonacci

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.sink.writer.DefaultFileWriterBucketFactory
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.execution.CheckpointingMode
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.AvroDeserializationSchema
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.RichMapFunction

private const val TOPIC = "ecommerce-avro-topic"
private const val GROUP_ID = "flink-parquet-consumer"
private const val OUTPUT_PATH = "/opt/flink/output/parquet"

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    env.enableCheckpointing(5000)
    env.checkpointConfig.checkpointingMode = CheckpointingMode.EXACTLY_ONCE
    env.checkpointConfig.minPauseBetweenCheckpoints = 2000
    env.checkpointConfig.checkpointTimeout = 60000
    env.checkpointConfig.maxConcurrentCheckpoints = 1
    env.checkpointConfig.setTolerableCheckpointFailureNumber(3)

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
            {"name":"InvoiceDate","type":["null","string"],"default":null},
            {"name":"UnitPrice","type":["null","double"],"default":null},
            {"name":"CustomerID","type":["null","string"],"default":null},
            {"name":"Country","type":["null","string"],"default":null}
          ]
        }
        """.trimIndent()
    )

    val kafkaSource = KafkaSource.builder<GenericRecord>()
        .setBootstrapServers("kafka:9092")
        .setTopics(TOPIC)
        .setGroupId(GROUP_ID)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(AvroDeserializationSchema.forGeneric(schema))
        .build()

    val avroTypeInfo = GenericRecordAvroTypeInfo(schema)

    val stream: DataStream<GenericRecord> = env
        .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
        .returns(avroTypeInfo)
        .map(object : RichMapFunction<GenericRecord, GenericRecord>() {
            override fun map(value: GenericRecord): GenericRecord {
                Thread.sleep(1)
                return value
            }
        })
        .returns(avroTypeInfo)
        .name("avro-pass-through")

    val sink = FileSink
        .forBulkFormat(
            Path(OUTPUT_PATH),
            AvroParquetWriters.forGenericRecord(schema)
        )
        .withBucketCheckInterval(1000)
        .withBucketAssigner { _, _ -> "bucket-0" }
        .withBucketFactory(DefaultFileWriterBucketFactory())
        .build()

    stream.sinkTo(sink).name("parquet-sink")

    env.execute("Kafka to Parquet with Flink")
}