package org.fibonacci

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.types.Row
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

    val rowType: TypeInformation<Row> = Types.ROW_NAMED(
        arrayOf(
            "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"
        ), Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.LONG, Types.DOUBLE, Types.STRING, Types.STRING
    )

    val source = KafkaSource.builder<ByteArray>().setBootstrapServers("kafka:9092").setTopics("ecommerce-avro-topic")
        .setGroupId("flink-avro-row-test").setStartingOffsets(OffsetsInitializer.earliest()).setDeserializer(
            KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer::class.java)
        ).build()

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source").map { bytes -> decodeToRow(bytes, schema) }
        .returns(rowType).map { row ->
            "invoice=${row.getField(0)}, stock=${row.getField(1)}, qty=${row.getField(3)}, customer=${row.getField(6)}, country=${
                row.getField(
                    7
                )
            }"
        }.print()

    env.execute("Kafka avro row test")
}

private fun decodeToRow(bytes: ByteArray, schema: Schema): Row {
    val reader = GenericDatumReader<GenericRecord>(schema)
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val record = reader.read(null, decoder)

    val row = Row(8)
    row.setField(0, valueAsString(record, "InvoiceNo"))
    row.setField(1, valueAsString(record, "StockCode"))
    row.setField(2, valueAsString(record, "Description"))
    row.setField(3, valueAsInt(record, "Quantity"))
    row.setField(4, valueAsLong(record, "InvoiceDate"))
    row.setField(5, valueAsDouble(record, "UnitPrice"))
    row.setField(6, valueAsString(record, "CustomerID"))
    row.setField(7, valueAsString(record, "Country"))
    return row
}

private fun valueAsString(record: GenericRecord, field: String): String? = record.get(field)?.toString()

private fun valueAsInt(record: GenericRecord, field: String): Int? = (record.get(field) as? Int)

private fun valueAsLong(record: GenericRecord, field: String): Long? = when (val v = record.get(field)) {
    is Long -> v
    is Int -> v.toLong()
    else -> null
}

private fun valueAsDouble(record: GenericRecord, field: String): Double? = when (val v = record.get(field)) {
    is Double -> v
    is Float -> v.toDouble()
    else -> null
}
