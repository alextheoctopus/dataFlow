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
