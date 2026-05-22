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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.time.Duration
import java.time.Instant

private data class UserEventForFlink(
    val eventId: String,
    val userId: String,
    val eventType: String,
    val eventTime: Long
)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    env.enableCheckpointing(5000)

    val schema = Schema.Parser().parse(EVENT_SCHEMA_JSON)

    val source = KafkaSource.builder<ByteArray>()
        .setBootstrapServers("kafka:9092")
        .setTopics(EVENTS_TOPIC_NAME)
        .setGroupId("flink-window-count-job")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(
            KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer::class.java)
        )
        .build()

    val events = env.fromSource(
        source,
        WatermarkStrategy.noWatermarks(),
        "kafka-events-source"
    )
        .map { bytes -> decodeEvent(bytes, schema) }
        .name("decode-avro-event")

    events
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .forBoundedOutOfOrderness<UserEventForFlink>(Duration.ofSeconds(5))
                .withTimestampAssigner { event, _ -> event.eventTime }
        )
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
        .allowedLateness(Time.seconds(30))
        .process(
            object : ProcessAllWindowFunction<UserEventForFlink, String, TimeWindow>() {
                override fun process(
                    context: Context,
                    elements: Iterable<UserEventForFlink>,
                    out: Collector<String>
                ) {
                    val count = elements.count()

                    val windowStart = Instant.ofEpochMilli(context.window().start)
                    val windowEnd = Instant.ofEpochMilli(context.window().end)

                    out.collect(
                        "WINDOW RESULT: [$windowStart - $windowEnd), count=$count"
                    )
                }
            }
        )
        .name("tumbling-window-count")
        .print()

    env.execute("Flink event-time tumbling window count")
}

private fun decodeEvent(bytes: ByteArray, schema: Schema): UserEventForFlink {
    val reader = GenericDatumReader<GenericRecord>(schema)
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val record = reader.read(null, decoder)

    return UserEventForFlink(
        eventId = record.get("event_id").toString(),
        userId = record.get("user_id").toString(),
        eventType = record.get("event_type").toString(),
        eventTime = record.get("event_time") as Long
    )
}