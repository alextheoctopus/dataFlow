package org.fibonacci

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.Properties
import kotlin.random.Random

const val EVENTS_TOPIC_NAME = "events-avro-topic"

const val EVENT_SCHEMA_JSON = """
{
  "type":"record",
  "name":"UserEvent",
  "namespace":"org.fibonacci",
  "fields":[
    {"name":"event_id","type":"string"},
    {"name":"user_id","type":"string"},
    {"name":"event_type","type":"string"},
    {"name":"event_time","type":"long"}
  ]
}
"""

private data class UserEvent(
    val eventId: String,
    val userId: String,
    val eventType: String,
    val eventTime: Long
)

private enum class ProducerMode {
    NORMAL,
    OUT_OF_ORDER,
    LATE
}

fun main() {
    val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:29092"
    val mode = System.getenv("PRODUCER_MODE")
        ?.uppercase()
        ?.replace("-", "_")
        ?.let { ProducerMode.valueOf(it) }
        ?: ProducerMode.NORMAL

    val eventCount = System.getenv("EVENT_COUNT")?.toIntOrNull() ?: 200
    val sleepMs = System.getenv("SEND_SLEEP_MS")?.toLongOrNull() ?: 100L

    val schema = Schema.Parser().parse(EVENT_SCHEMA_JSON)

    createEventsTopicIfMissing(bootstrapServers)

    val producerProps = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.LINGER_MS_CONFIG, "10")
    }

    val random = Random(System.currentTimeMillis())
    val eventTypes = listOf("click", "view", "purchase", "login")

    val reorderBuffer = mutableListOf<UserEvent>()
    val delayedQueue = mutableListOf<UserEvent>()

    val baseTime = Instant.now().toEpochMilli()

    KafkaProducer<String, ByteArray>(producerProps).use { producer ->
        for (i in 1..eventCount) {
            val event = UserEvent(
                eventId = "event-$i",
                userId = "user-${random.nextInt(1, 6)}",
                eventType = eventTypes.random(random),
                eventTime = baseTime + i * 1000L
            )

            when (mode) {
                ProducerMode.NORMAL -> {
                    sendEvent(producer, schema, event, "normal")
                }

                ProducerMode.OUT_OF_ORDER -> {
                    reorderBuffer.add(event)

                    if (reorderBuffer.size >= 8) {
                        val indexToSend = if (random.nextDouble() < 0.25) {
                            random.nextInt(reorderBuffer.size)
                        } else {
                            0
                        }

                        val eventToSend = reorderBuffer.removeAt(indexToSend)
                        sendEvent(producer, schema, eventToSend, "out-of-order")
                    }
                }

                ProducerMode.LATE -> {
                    if (random.nextDouble() < 0.15) {
                        delayedQueue.add(event)
                        println("DELAYED: ${event.eventId}, event_time=${event.eventTime}")
                    } else {
                        sendEvent(producer, schema, event, "normal")
                    }

                    if (i % 20 == 0 && delayedQueue.isNotEmpty()) {
                        val lateEvent = delayedQueue.removeAt(0)
                        sendEvent(producer, schema, lateEvent, "late")
                    }
                }
            }

            Thread.sleep(sleepMs)
        }

        reorderBuffer.forEach { sendEvent(producer, schema, it, "flush-buffer") }
        delayedQueue.forEach { sendEvent(producer, schema, it, "flush-late") }

        producer.flush()
        println("Finished. Mode=$mode, generated=$eventCount")
    }
}

private fun sendEvent(
    producer: KafkaProducer<String, ByteArray>,
    schema: Schema,
    event: UserEvent,
    marker: String
) {
    val record = GenericData.Record(schema).apply {
        put("event_id", event.eventId)
        put("user_id", event.userId)
        put("event_type", event.eventType)
        put("event_time", event.eventTime)
    }

    val payload = avroEventToBytes(record, schema)

    producer.send(
        ProducerRecord(
            EVENTS_TOPIC_NAME,
            event.userId,
            payload
        )
    )

    println(
        "SEND [$marker]: id=${event.eventId}, user=${event.userId}, type=${event.eventType}, event_time=${event.eventTime}"
    )
}

private fun createEventsTopicIfMissing(bootstrapServers: String) {
    val adminProps = Properties().apply {
        put("bootstrap.servers", bootstrapServers)
    }

    AdminClient.create(adminProps).use { admin ->
        val existing = admin.listTopics().names().get()

        if (EVENTS_TOPIC_NAME !in existing) {
            admin.createTopics(listOf(NewTopic(EVENTS_TOPIC_NAME, 1, 1))).all().get()
            println("Topic $EVENTS_TOPIC_NAME created.")
        }
    }
}

private fun avroEventToBytes(record: GenericData.Record, schema: Schema): ByteArray {
    val writer = GenericDatumWriter<GenericData.Record>(schema)
    val output = ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(output, null)

    writer.write(record, encoder)
    encoder.flush()

    return output.toByteArray()
}