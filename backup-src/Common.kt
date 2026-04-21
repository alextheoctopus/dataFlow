package org.fibonacci

const val TOPIC_NAME = "ecommerce-avro-topic"
const val GROUP_ID = "flink-parquet-consumer"

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
