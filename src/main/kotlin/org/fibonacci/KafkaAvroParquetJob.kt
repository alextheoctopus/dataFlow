package org.fibonacci

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.ByteArrayDeserializer
/*
Поднять Flink-окружение и включить checkpoints.
Описать схему входных Kafka данных.
Описать схему выходных parquet-данных.
Подключиться к Kafka и прочитать сообщения как ByteArray.
Для каждой записи:
попробовать декодировать bytes как Avro,
преобразовать поля в безопасный вид,
собрать новый GenericRecord по sinkSchema,
если запись битая, просто пропуск.
Поток валидных записей пишет в parquet.
Файлы перекатываются по checkpoint.
Job запускается и становится пригодной для savepoint/restore.*/
fun main() {
    //объект окружения flink
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    //чекпоинты кажды 5 сек
    env.enableCheckpointing(5000)
//Авро схема для входных данных-прочитать байты в kafka-message
    val sourceSchema = Schema.Parser().parse(
        """
        {
          "type": "record",
          "name": "EcommerceSourceRecord",
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
//Схема для данных для записи в паркет
    val sinkSchema = Schema.Parser().parse(
        """
        {
          "type": "record",
          "name": "EcommerceParquetRecord",
          "namespace": "org.fibonacci",
          "fields": [
            {"name":"invoiceNo","type":["null","string"],"default":null},
            {"name":"stockCode","type":["null","string"],"default":null},
            {"name":"description","type":["null","string"],"default":null},
            {"name":"quantity","type":["null","int"],"default":null},
            {"name":"invoiceDate","type":["null","long"],"default":null},
            {"name":"unitPrice","type":["null","double"],"default":null},
            {"name":"customerId","type":["null","string"],"default":null},
            {"name":"country","type":["null","string"],"default":null}
          ]
        }
        """.trimIndent()
    )
// Откуда мы читаем данные
    val source = KafkaSource.builder<ByteArray>()
        .setBootstrapServers("kafka:9092")
        .setTopics("ecommerce-avro-topic")
        .setGroupId("flink-avro-parquet-job")
        .setStartingOffsets(OffsetsInitializer.earliest())//читать данные с самых ранних доступных offsets -
        //для нас удобно, потому что у нас датасет
        //десериализуем value байты
        .setDeserializer(
            KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer::class.java)
        )
        .build()
//поток данных из источника
    val stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
        //flatMap не обязан возвращать объект всегда
        .flatMap { bytes: ByteArray, out: Collector<GenericRecord> ->
            //обработка записи
            try {
                val record = decodeToGenericRecord(bytes, sourceSchema, sinkSchema)
                //успех? дальше по пайплайну флинка
                out.collect(record)
            } catch (_: Exception) {
                // Пропускаем битую/невалидную запись, job не падает
            }
        }
        //исключаем kryo сериализацию
        .returns(GenericRecordAvroTypeInfo(sinkSchema))
        .name("decode-avro-to-generic-record")


    val sink = FileSink
        .forBulkFormat(//пишем как бинарные паркет файл
            Path("/opt/flink/output/parquet"),
            AvroParquetWriters.forGenericRecord(sinkSchema)
        )
        .withRollingPolicy(OnCheckpointRollingPolicy.build())//для корректного checkpoint/savepoint без потери данных
        .build()
//Поток stream → отправить в sink
    stream.sinkTo(sink).name("parquet-sink")
//Запуск пайплайна описанного сверху
    env.execute("Kafka avro parquet job")
}

/*Вспомогательные функции*/
//формирование GenericRecord для записи в sink
private fun decodeToGenericRecord(
    bytes: ByteArray,
    sourceSchema: Schema,
    sinkSchema: Schema
): GenericRecord {
    //Avro reader для чтения входной записи по sourceSchema
    val reader = GenericDatumReader<GenericRecord>(sourceSchema)
    //бинарный Avro decoder поверх массива байт
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    //реальное чтение bytes
    val sourceRecord = reader.read(null, decoder)

    return GenericData.Record(sinkSchema).apply {
        put("invoiceNo", safeString(sourceRecord, "InvoiceNo"))
        put("stockCode", safeString(sourceRecord, "StockCode"))
        put("description", safeString(sourceRecord, "Description"))
        put("quantity", safeInt(sourceRecord, "Quantity"))
        put("invoiceDate", safeLong(sourceRecord, "InvoiceDate"))
        put("unitPrice", safeDouble(sourceRecord, "UnitPrice"))
        put("customerId", safeString(sourceRecord, "CustomerID"))
        put("country", safeString(sourceRecord, "Country"))
    }
}
/*безопасно достаем поля*/
private fun safeString(record: GenericRecord, field: String): String? {
    val value = record.get(field) ?: return null
    val text = value.toString().trim()
    return text.ifEmpty { null }
}

private fun safeInt(record: GenericRecord, field: String): Int? =
    when (val value = record.get(field)) {
        is Int -> value
        is Long -> value.toInt()
        is String -> value.toIntOrNull()
        else -> null
    }

private fun safeLong(record: GenericRecord, field: String): Long? =
    when (val value = record.get(field)) {
        is Long -> value
        is Int -> value.toLong()
        is String -> value.toLongOrNull()
        else -> null
    }

private fun safeDouble(record: GenericRecord, field: String): Double? =
    when (val value = record.get(field)) {
        is Double -> if (value.isFinite()) value else null
        is Float -> value.toDouble().takeIf { it.isFinite() }
        is Int -> value.toDouble()
        is Long -> value.toDouble()
        is String -> value.toDoubleOrNull()?.takeIf { it.isFinite() }
        else -> null
    }