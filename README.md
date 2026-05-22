1. Команды в ps
2. Пересобрать jar:

```powershell
$p=(Get-Location).Path; docker rm -f flink_build_temp 2>$null; docker run --name flink_build_temp --dns 8.8.8.8 --dns 1.1.1.1 -e GRADLE_USER_HOME=/tmp/gradle -v "${p}:/src:ro" gradle:8.10.2-jdk17 bash -lc "mkdir -p /tmp/app && cd /src && tar --exclude=.gradle --exclude=build --exclude=build-out -cf - . | tar -xf - -C /tmp/app && cd /tmp/app && gradle --no-daemon --console=plain clean shadowJar"
```

3. Забрать jar:

```powershell
New-Item -ItemType Directory -Force .\build\libs | Out-Null; docker cp flink_build_temp:/tmp/app/build/libs/flink-kafka-parquet-lab.jar .\build\libs\
```

---



## 1. Демонстрация пайплайна с нуля

2. Полностью остановить окружение:

```powershell
docker compose down -v --remove-orphans
```

3. Очистить локальные папки результата:

```powershell
Remove-Item -Recurse -Force .\parquet -ErrorAction SilentlyContinue; Remove-Item -Recurse -Force .\checkpoints -ErrorAction SilentlyContinue; Remove-Item -Recurse -Force .\savepoints -ErrorAction SilentlyContinue; New-Item -ItemType Directory -Force .\parquet | Out-Null; New-Item -ItemType Directory -Force .\checkpoints | Out-Null; New-Item -ItemType Directory -Force .\savepoints | Out-Null
```

4. Поднять Kafka и Flink:

```powershell
docker compose up -d
```

5. Проверить контейнеры:

```powershell
docker ps
```

6. Отправить CSV в Kafka:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"; java -cp build/libs/flink-kafka-parquet-lab.jar org.fibonacci.KafkaProducerKt
```

7. Запустить Flink job, который читает Kafka и пишет parquet:

```powershell
docker exec -it flink-jobmanager flink run -d -c org.fibonacci.KafkaAvroParquetJobKt /opt/flink/usrlib/flink-kafka-parquet-lab.jar
```

8. Посмотреть job id:

```powershell
docker exec -it flink-jobmanager flink list
```

9. Открыть Flink UI:

```powershell
"http://localhost:8081"
```

10. Подождать, чтобы появились checkpoints и файлы - 15-20 секунд

11. Проверить parquet-файлы:

```powershell
Get-ChildItem .\parquet -Recurse
```

12. Ещё раз проверить, что job работает:

```powershell
docker exec -it flink-jobmanager flink list
```

13. Создать savepoint, подставив JOB_ID:

```powershell
docker exec -it flink-jobmanager flink savepoint JOB_ID file:///opt/flink/savepoints
```

14. Проверить путь savepoint:

```powershell
docker exec -it flink-jobmanager sh -lc "find /opt/flink/savepoints -maxdepth 3 | sort"
```

15. Остановить job, подставив тот же `JOB_ID`:

```powershell
docker exec -it flink-jobmanager flink cancel JOB_ID
```

16. Проверить, что job остановился:

```powershell
docker exec -it flink-jobmanager flink list
```

17. Поднять job заново из savepoint, подставив `SAVEPOINT_PATH`:

```powershell
docker exec -it flink-jobmanager flink run -d -s SAVEPOINT_PATH -c org.fibonacci.KafkaAvroParquetJobKt /opt/flink/usrlib/flink-kafka-parquet-lab.jar
```

18. Проверить, что restore сработал и job снова RUNNING:

```powershell
docker exec -it flink-jobmanager flink list
```

19. Подождать немного после restore - 10 секунд 

20. проверить parquet-файлы:

```powershell
Get-ChildItem .\parquet -Recurse
```


22. Подсчитать количество строк в parquet:

```powershell
java -cp build/libs/flink-kafka-parquet-lab.jar org.fibonacci.ParquetCheckKt
```

---

3 Лабораторная

Необходимо написать producer'а который генерирует события и отправляет их в Kafka.
Flink читает события, обрабатывает их по event time и считает оконные агрегаты - общее количество сообщение в окне

Каждое событие должно содержать:
- event_id
- user_id
- event_type
- event_time

Producer должен поддерживать три режима:
1. Обычная отправка: большинство событий идут по порядку.
2. Out-of-order: иногда более старое событие отправляется позже новых.
3. Поздние события: часть событий специально задерживается и отправляется позже.

Рекомендуемый способ реализации:
- держать буфер из 5-10 событий;
- 20-30% событий отправлять из буфера не по порядку (Out-of-order);
- 10-15% событий откладывать в очередь delayed и отправлять позже. Или делать sleep() перед отправкой

Flink должен уметь:
- читать события из Kafka;
- извлекать event_time;
- настраивать watermarks;
- обрабатывать поздние события (allowed lateness);
- считать общее количество сообщений в окне (только Tumbling Window)

Результаты окон необходимо выводить в консоль по мере срабатывания окон.
| Требование         | Где                                          |
| ------------------ | ---------------------------------------------------- |
| `event_id`         | `EventProducer.kt`                                   |
| `user_id`          | `EventProducer.kt`                                   |
| `event_type`       | `EventProducer.kt`                                   |
| `event_time`       | `EventProducer.kt`                                   |
| Обычная отправка   | `PRODUCER_MODE=NORMAL`                               |
| Out-of-order       | `PRODUCER_MODE=OUT_OF_ORDER`                         |
| Поздние события    | `PRODUCER_MODE=LATE`                                 |
| Flink читает Kafka | `FlinkWindowCountJob.kt`                             |
| Event time         | `withTimestampAssigner { event -> event.eventTime }` |
| Watermarks         | `forBoundedOutOfOrderness(Duration.ofSeconds(5))`    |
| Allowed lateness   | `.allowedLateness(Time.seconds(30))`                 |
| Tumbling Window    | `TumblingEventTimeWindows.of(Time.seconds(10))`      |

Команды

$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"; $env:PRODUCER_MODE="NORMAL"; $env:EVENT_COUNT="20"; $env:SEND_SLEEP_MS="100"; java -cp build/libs/flink-kafka-parquet-lab.jar org.fibonacci.EventProducerKt

проверить

docker exec -it kafka bash -lc "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list"

docker exec -it flink-jobmanager flink list

запуск flinkjob
``powershell
docker exec -it flink-jobmanager flink run -d -c org.fibonacci.FlinkWindowCountJobKt /opt/flink/usrlib/flink-kafka-parquet-lab.jar
``

проверить

docker exec -it flink-jobmanager flink list
результат
------------------ Running/Restarting Jobs -------------------
22.05.2026 17:12:43 : ef0558a88d091480cb8ac79d4813d7be : Flink event-time tumbling window count (RUNNING)

команда запуска
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"; $env:PRODUCER_MODE="NORMAL"; $env:EVENT_COUNT="100"; $env:SEND_SLEEP_MS="100"; java -cp build/libs/flink-kafka-parquet-lab.jar org.fibonacci.EventProducerKt

проверка
docker logs flink-taskmanager --tail 200

WINDOW RESULT: [2026-05-22T17:07:50Z - 2026-05-22T17:08:00Z), count=10
WINDOW RESULT: [2026-05-22T17:14:20Z - 2026-05-22T17:14:30Z), count=6
WINDOW RESULT: [2026-05-22T17:14:30Z - 2026-05-22T17:14:40Z), count=10
WINDOW RESULT: [2026-05-22T17:14:40Z - 2026-05-22T17:14:50Z), count=10
WINDOW RESULT: [2026-05-22T17:14:50Z - 2026-05-22T17:15:00Z), count=10
WINDOW RESULT: [2026-05-22T17:15:00Z - 2026-05-22T17:15:10Z), count=10
WINDOW RESULT: [2026-05-22T17:15:10Z - 2026-05-22T17:15:20Z), count=10
WINDOW RESULT: [2026-05-22T17:15:20Z - 2026-05-22T17:15:30Z), count=10
WINDOW RESULT: [2026-05-22T17:15:30Z - 2026-05-22T17:15:40Z), count=10
WINDOW RESULT: [2026-05-22T17:15:40Z - 2026-05-22T17:15:50Z), count=10

//Flink читает из Kafka
извлекает event_time
считает Tumbling Window
выводит count в консоль//

Out-of-order режим

команда запуска
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"; $env:PRODUCER_MODE="OUT_OF_ORDER"; $env:EVENT_COUNT="100"; $env:SEND_SLEEP_MS="100"; java -cp build/libs/flink-kafka-parquet-lab.jar org.fibonacci.EventProducerKt

docker logs flink-taskmanager --tail 200

WINDOW RESULT: [2026-05-22T17:07:50Z - 2026-05-22T17:08:00Z), count=10
WINDOW RESULT: [2026-05-22T17:14:20Z - 2026-05-22T17:14:30Z), count=6
WINDOW RESULT: [2026-05-22T17:14:30Z - 2026-05-22T17:14:40Z), count=10
WINDOW RESULT: [2026-05-22T17:14:40Z - 2026-05-22T17:14:50Z), count=10
WINDOW RESULT: [2026-05-22T17:14:50Z - 2026-05-22T17:15:00Z), count=10
WINDOW RESULT: [2026-05-22T17:15:00Z - 2026-05-22T17:15:10Z), count=10
WINDOW RESULT: [2026-05-22T17:15:10Z - 2026-05-22T17:15:20Z), count=10
WINDOW RESULT: [2026-05-22T17:15:20Z - 2026-05-22T17:15:30Z), count=10
WINDOW RESULT: [2026-05-22T17:15:30Z - 2026-05-22T17:15:40Z), count=10
WINDOW RESULT: [2026-05-22T17:15:40Z - 2026-05-22T17:15:50Z), count=10
WINDOW RESULT: [2026-05-22T17:15:50Z - 2026-05-22T17:16:00Z), count=10
WINDOW RESULT: [2026-05-22T17:16:00Z - 2026-05-22T17:16:10Z), count=4
WINDOW RESULT: [2026-05-22T17:17:10Z - 2026-05-22T17:17:20Z), count=9
WINDOW RESULT: [2026-05-22T17:17:20Z - 2026-05-22T17:17:30Z), count=10
WINDOW RESULT: [2026-05-22T17:17:30Z - 2026-05-22T17:17:40Z), count=10
WINDOW RESULT: [2026-05-22T17:17:40Z - 2026-05-22T17:17:50Z), count=8
WINDOW RESULT: [2026-05-22T17:17:40Z - 2026-05-22T17:17:50Z), count=9
WINDOW RESULT: [2026-05-22T17:17:40Z - 2026-05-22T17:17:50Z), count=10
WINDOW RESULT: [2026-05-22T17:17:50Z - 2026-05-22T17:18:00Z), count=10
WINDOW RESULT: [2026-05-22T17:18:00Z - 2026-05-22T17:18:10Z), count=10
WINDOW RESULT: [2026-05-22T17:18:10Z - 2026-05-22T17:18:20Z), count=10
WINDOW RESULT: [2026-05-22T17:18:20Z - 2026-05-22T17:18:30Z), count=10
WINDOW RESULT: [2026-05-22T17:18:30Z - 2026-05-22T17:18:40Z), count=10

late-events

команда запуска
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"; $env:PRODUCER_MODE="LATE"; $env:EVENT_COUNT="100"; $env:SEND_SLEEP_MS="100"; java -cp build/libs/flink-kafka-parquet-lab.jar org.fibonacci.EventProducerKt

DELAYED: event-3
DELAYED: event-14
DELAYED: event-16
...
SEND [late]: id=event-3
SEND [late]: id=event-14
SEND [late]: id=event-16
...
SEND [flush-late]: id=event-92

проверка
docker logs flink-taskmanager --tail 250

WINDOW RESULT: [2026-05-22T17:07:40Z - 2026-05-22T17:07:50Z), count=10
2026-05-22 17:13:06,038 WARN  org.apache.flink.connector.kafka.source.reader.KafkaSourceReader [] - Failed to commit consumer offsets for checkpoint 2
org.apache.kafka.clients.consumer.RetriableCommitFailedException: Offset commit fail
ed with a retriable exception. You should retry committing the latest consumed offsets.
Caused by: org.apache.kafka.common.errors.CoordinatorNotAvailableException: The coordinator is not available.
2026-05-22 17:13:06,040 WARN  org.apache.flink.connector.kafka.source.reader.KafkaSourceReader [] - Failed to commit consumer offsets for checkpoint 3
org.apache.kafka.clients.consumer.RetriableCommitFailedException: Offset commit fail
ed with a retriable exception. You should retry committing the latest consumed offsets.
Caused by: org.apache.kafka.common.errors.CoordinatorNotAvailableException: The coordinator is not available.
2026-05-22 17:13:16,179 INFO  org.apache.kafka.clients.consumer.internals.ConsumerCo
ordinator [] - [Consumer clientId=flink-window-count-job-0, groupId=flink-window-count-job] Discovered group coordinator kafka:9092 (id: 2147483646 rack: null)
WINDOW RESULT: [2026-05-22T17:07:50Z - 2026-05-22T17:08:00Z), count=10
WINDOW RESULT: [2026-05-22T17:14:20Z - 2026-05-22T17:14:30Z), count=6
WINDOW RESULT: [2026-05-22T17:14:30Z - 2026-05-22T17:14:40Z), count=10
WINDOW RESULT: [2026-05-22T17:14:40Z - 2026-05-22T17:14:50Z), count=10
WINDOW RESULT: [2026-05-22T17:14:50Z - 2026-05-22T17:15:00Z), count=10
WINDOW RESULT: [2026-05-22T17:15:00Z - 2026-05-22T17:15:10Z), count=10
WINDOW RESULT: [2026-05-22T17:15:10Z - 2026-05-22T17:15:20Z), count=10
WINDOW RESULT: [2026-05-22T17:15:20Z - 2026-05-22T17:15:30Z), count=10
WINDOW RESULT: [2026-05-22T17:15:30Z - 2026-05-22T17:15:40Z), count=10
WINDOW RESULT: [2026-05-22T17:15:40Z - 2026-05-22T17:15:50Z), count=10
WINDOW RESULT: [2026-05-22T17:15:50Z - 2026-05-22T17:16:00Z), count=10
WINDOW RESULT: [2026-05-22T17:16:00Z - 2026-05-22T17:16:10Z), count=4
WINDOW RESULT: [2026-05-22T17:17:10Z - 2026-05-22T17:17:20Z), count=9
WINDOW RESULT: [2026-05-22T17:17:20Z - 2026-05-22T17:17:30Z), count=10
WINDOW RESULT: [2026-05-22T17:17:30Z - 2026-05-22T17:17:40Z), count=10
WINDOW RESULT: [2026-05-22T17:17:40Z - 2026-05-22T17:17:50Z), count=8
WINDOW RESULT: [2026-05-22T17:17:40Z - 2026-05-22T17:17:50Z), count=9
WINDOW RESULT: [2026-05-22T17:17:40Z - 2026-05-22T17:17:50Z), count=10
WINDOW RESULT: [2026-05-22T17:17:50Z - 2026-05-22T17:18:00Z), count=10
WINDOW RESULT: [2026-05-22T17:18:00Z - 2026-05-22T17:18:10Z), count=10
WINDOW RESULT: [2026-05-22T17:18:10Z - 2026-05-22T17:18:20Z), count=10
WINDOW RESULT: [2026-05-22T17:18:20Z - 2026-05-22T17:18:30Z), count=10
WINDOW RESULT: [2026-05-22T17:18:30Z - 2026-05-22T17:18:40Z), count=10
WINDOW RESULT: [2026-05-22T17:18:40Z - 2026-05-22T17:18:50Z), count=10
WINDOW RESULT: [2026-05-22T17:18:50Z - 2026-05-22T17:19:00Z), count=1
WINDOW RESULT: [2026-05-22T17:19:10Z - 2026-05-22T17:19:20Z), count=7
WINDOW RESULT: [2026-05-22T17:19:10Z - 2026-05-22T17:19:20Z), count=8
WINDOW RESULT: [2026-05-22T17:19:20Z - 2026-05-22T17:19:30Z), count=8
WINDOW RESULT: [2026-05-22T17:19:30Z - 2026-05-22T17:19:40Z), count=9
WINDOW RESULT: [2026-05-22T17:19:20Z - 2026-05-22T17:19:30Z), count=9
WINDOW RESULT: [2026-05-22T17:19:40Z - 2026-05-22T17:19:50Z), count=8
WINDOW RESULT: [2026-05-22T17:19:50Z - 2026-05-22T17:20:00Z), count=9
WINDOW RESULT: [2026-05-22T17:20:00Z - 2026-05-22T17:20:10Z), count=9
WINDOW RESULT: [2026-05-22T17:20:10Z - 2026-05-22T17:20:20Z), count=10
WINDOW RESULT: [2026-05-22T17:20:20Z - 2026-05-22T17:20:30Z), count=9
WINDOW RESULT: [2026-05-22T17:20:30Z - 2026-05-22T17:20:40Z), count=10
WINDOW RESULT: [2026-05-22T17:20:20Z - 2026-05-22T17:20:30Z), count=10


NORMAL — producer отправил 100 обычных событий.
OUT_OF_ORDER — Flink обновлял одно и то же окно несколько раз.
LATE — producer выводил DELAYED, SEND [late], SEND [flush-late], а Flink пересчитывал окна.