package org.fibonacci

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import java.io.File

fun main() {
    val root = File("parquet")

    if (!root.exists()) {
        println("Folder not found: ${root.absolutePath}")
        return
    }

    val parquetFiles = root.walkTopDown()
        .filter { it.isFile }
        .filter { it.name.startsWith("part-") }
        .filter { !it.name.contains(".inprogress") }
        .filter { it.length() > 0L }
        .toList()

    if (parquetFiles.isEmpty()) {
        println("No parquet data files found in ${root.path}")
        return
    }

    var total = 0L

    for (file in parquetFiles) {
        val reader = AvroParquetReader.builder<GenericRecord>(Path(file.absolutePath))
            .withConf(Configuration())
            .build()

        var fileCount = 0L
        reader.use {
            while (true) {
                val record = it.read() ?: break
                fileCount++
            }
        }

        println("${file.name}: $fileCount")
        total += fileCount
    }

    println("Parquet row count: $total")
}