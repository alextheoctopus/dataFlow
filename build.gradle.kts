plugins {
    kotlin("jvm") version "1.9.25"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    mavenCentral()
}

val flinkVersion = "1.20.1"
val flinkKafkaConnectorVersion = "3.4.0-1.20"
val kafkaClientsVersion = "3.9.0"
val avroVersion = "1.11.4"
val parquetVersion = "1.14.4"
val hadoopVersion = "3.3.6"
val commonsCsvVersion = "1.11.0"

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("org.fibonacci.FlinkKafkaToParquetKt")
}

dependencies {
    implementation(kotlin("stdlib"))

    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:$flinkKafkaConnectorVersion")
    implementation("org.apache.flink:flink-connector-files:$flinkVersion")
    implementation("org.apache.flink:flink-parquet:$flinkVersion")
    implementation("org.apache.flink:flink-avro:$flinkVersion")

    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
    implementation("org.apache.avro:avro:$avroVersion")
    implementation("org.apache.parquet:parquet-avro:$parquetVersion")
    implementation("org.apache.hadoop:hadoop-common:$hadoopVersion")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion")
    implementation("org.apache.commons:commons-csv:$commonsCsvVersion")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
    isZip64 = true
}


