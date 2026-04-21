package org.fibonacci

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Properties

private val invoiceDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

//имя кафка топика куда идут отправляться данные
const val TOPIC_NAME = "ecommerce-avro-topic"
const val GROUP_ID = "flink-parquet-consumer"

//вид каждой записи отправляемой в кафку
const val AVRO_SCHEMA_JSON = """
{
  "type":"record",
  "name":"EcommerceRow",
  "namespace":"org.fibonacci",
  "fields":[
    {"name":"InvoiceNo","type":["null","string"],"default":null},
    {"name":"StockCode","type":["null","string"],"default":null},
    {"name":"Description","type":["null","string"],"default":null},
    {"name":"Quantity","type":["null","int"],"default":null},
    {"name":"InvoiceDate","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null},
    {"name":"UnitPrice","type":["null","double"],"default":null},
    {"name":"CustomerID","type":["null","string"],"default":null},
    {"name":"Country","type":["null","string"],"default":null}
  ]
}
"""

fun main() {
    val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:29092"
    val csvPath = System.getenv("DATASET_PATH") ?: "datasets/ecommercedataset.csv"

    //объект Schema, с которым умеет работать Avro API
    val schema = Schema.Parser().parse(AVRO_SCHEMA_JSON)
    //Подключаемся к кафке админом, создаем топик
    createTopicIfMissing(bootstrapServers)
//настройка продьюсера
    val producerProps = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)//kafka key - строка
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)//kafka value - byteArray
        put(ProducerConfig.ACKS_CONFIG, "all")//запись должна быть подтверждена всеми
        put(ProducerConfig.LINGER_MS_CONFIG, "50")
    }

    KafkaProducer<String, ByteArray>(producerProps).use { producer ->
        File(csvPath).inputStream().use { input ->//файл как входной поток данных
            InputStreamReader(input, StandardCharsets.ISO_8859_1).use { reader ->// преобразовать к кодировке файла
                CSVParser(
                    reader,
                    CSVFormat.DEFAULT.builder()
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .build()
                ).use { parser ->
                    var sent = 0
                    for (row in parser) {
                        val invoiceNo = row.get("InvoiceNo").nullIfBlank() ?: continue
                        val stockCode = row.get("StockCode").nullIfBlank() ?: continue

                        val record = GenericData.Record(schema).apply {
                            put("InvoiceNo", invoiceNo)
                            put("StockCode", stockCode)
                            put("Description", row.get("Description").nullIfBlank())
                            put("Quantity", row.get("Quantity").nullIfBlank()?.toInt())
                            put("InvoiceDate", row.get("InvoiceDate").nullIfBlank()?.let(::parseInvoiceDateToMillis))
                            put("UnitPrice", row.get("UnitPrice").nullIfBlank()?.toDouble())
                            put("CustomerID", row.get("CustomerID").nullIfBlank())
                            put("Country", row.get("Country").nullIfBlank())
                        }

                        val payload = avroToBytes(record, schema)//Сериализация в байты
                        //kafka ключ
                        val key = row.get("CustomerID").nullIfBlank() ?: invoiceNo
                        //отправка по строке в кафку
                        producer.send(ProducerRecord(TOPIC_NAME, key, payload))
                        sent++

                        if (sent % 50_000 == 0) {
                            println("Sent $sent records...")
                        }
                    }
                    //отправлем все остатки в буферах
                    producer.flush()
                    println("Kafka write finished. Sent $sent records.")
                }
            }
        }
    }
}

private fun createTopicIfMissing(bootstrapServers: String) {
    //Настраиваем AdminClient ← кафка адрес
    val adminProps = Properties().apply {
        put("bootstrap.servers", bootstrapServers)
    }

    AdminClient.create(adminProps).use { admin ->
        val existing = admin.listTopics().names().get()
        if (TOPIC_NAME !in existing) {
            admin.createTopics(listOf(NewTopic(TOPIC_NAME, 1, 1))).all().get()
            println("Topic $TOPIC_NAME created.")
        }
    }
}

private fun avroToBytes(record: GenericData.Record, schema: Schema): ByteArray {
    //Создаем Avro writer по нашей схеме
    val writer = GenericDatumWriter<GenericData.Record>(schema)
    //Буфер для байт
    val output = ByteArrayOutputStream()
    //авро энкодер для записи в output
    val encoder = EncoderFactory.get().binaryEncoder(output, null)
    //запись record в бинарном формате
    writer.write(record, encoder)
    //дозапись остатков
    encoder.flush()
    return output.toByteArray()
}
//получает строку даты и возвращает миллисекунды в UTC,чтобы соответствовать Avro logical type timestamp-millis
private fun parseInvoiceDateToMillis(value: String): Long {
    return LocalDateTime.parse(value, invoiceDateFormatter)
        .toInstant(ZoneOffset.UTC)
        .toEpochMilli()
}

private fun String?.nullIfBlank(): String? = this?.trim()?.takeIf { it.isNotEmpty() }
