package org.fibonacci

import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.Collector
import kotlin.random.Random

data class Lab4Event(
    val userId: Int = 0,
    val eventType: String = "",
    val value: Double = 0.0,
    val timestamp: Long = 0L
)

data class Lab4Rule(
    val blockedUser: Int = 0
)

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    env.setParallelism(1)
    env.enableCheckpointing(5000)

    val blockedUsersDescriptor = MapStateDescriptor(
        "blocked-users",
        Types.INT,
        Types.BOOLEAN
    )

    val events = env
        .addSource(Lab4EventsSource())
        .name("events-source")

    val rules = env
        .addSource(Lab4RulesSource())
        .name("rules-source")

    val broadcastRules = rules.broadcast(blockedUsersDescriptor)

    events
        .keyBy { event -> event.userId }
        .connect(broadcastRules)
        .process(UserStatsWithRulesFunction(blockedUsersDescriptor))
        .name("user-stats-with-blocked-rules")
        .print()

    env.execute("Lab 4 two streams state ttl job")
}

class Lab4EventsSource : RichParallelSourceFunction<Lab4Event>() {
    @Volatile
    private var running = true

    override fun run(ctx: SourceFunction.SourceContext<Lab4Event>) {
        var userId = 1
        val eventTypes = listOf("click", "view", "purchase", "login")

        while (running) {
            val event = Lab4Event(
                userId = userId,
                eventType = eventTypes[(userId - 1) % eventTypes.size],
                value = Random.nextDouble(1.0, 10.0),
                timestamp = System.currentTimeMillis()
            )

            synchronized(ctx.checkpointLock) {
                ctx.collect(event)
            }

            userId = if (userId == 100) 1 else userId + 1

            Thread.sleep(50)
        }
    }

    override fun cancel() {
        running = false
    }
}

class Lab4RulesSource : RichParallelSourceFunction<Lab4Rule>() {
    @Volatile
    private var running = true

    override fun run(ctx: SourceFunction.SourceContext<Lab4Rule>) {
        while (running) {
            Thread.sleep(7000)

            val rule = Lab4Rule(
                blockedUser = Random.nextInt(1, 101)
            )

            synchronized(ctx.checkpointLock) {
                ctx.collect(rule)
            }
        }
    }

    override fun cancel() {
        running = false
    }
}

class UserStatsWithRulesFunction(
    private val blockedUsersDescriptor: MapStateDescriptor<Int, Boolean>
) : KeyedBroadcastProcessFunction<Int, Lab4Event, Lab4Rule, String>() {

    private lateinit var valuesState: ListState<Double>
    private lateinit var lastPrintTimeState: ValueState<Long>

    override fun open(parameters: Configuration) {
        val ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(10))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()

        val valuesDescriptor = ListStateDescriptor(
            "user-values",
            Types.DOUBLE
        )

        valuesDescriptor.enableTimeToLive(ttlConfig)

        valuesState = runtimeContext.getListState(valuesDescriptor)

        val lastPrintDescriptor = ValueStateDescriptor(
            "last-print-time",
            Types.LONG
        )

        lastPrintTimeState = runtimeContext.getState(lastPrintDescriptor)
    }

    override fun processElement(
        event: Lab4Event,
        ctx: ReadOnlyContext,
        out: Collector<String>
    ) {
        val blockedUsers = ctx.getBroadcastState(blockedUsersDescriptor)
        val isBlocked = blockedUsers.contains(event.userId)

        if (isBlocked) {
            out.collect(
                "BLOCKED EVENT IGNORED: userId=${event.userId}, eventType=${event.eventType}, value=${"%.2f".format(event.value)}"
            )
            return
        }

        valuesState.add(event.value)

        val values = valuesState.get().toList()
        val sum = values.sum()

        val now = System.currentTimeMillis()
        val lastPrintTime = lastPrintTimeState.value() ?: 0L

        if (now - lastPrintTime >= 5000) {
            out.collect(
                "ACTIVE USER SUM: userId=${event.userId}, sum=${"%.2f".format(sum)}, valuesInState=${values.size}, lastEventType=${event.eventType}, eventTimestamp=${event.timestamp}"
            )

            lastPrintTimeState.update(now)
        }
    }

    override fun processBroadcastElement(
        rule: Lab4Rule,
        ctx: Context,
        out: Collector<String>
    ) {
        val blockedUsers = ctx.getBroadcastState(blockedUsersDescriptor)

        blockedUsers.put(rule.blockedUser, true)

        out.collect("RULE UPDATE: blockedUser=${rule.blockedUser}")
    }
}