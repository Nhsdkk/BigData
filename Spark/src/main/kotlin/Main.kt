package org.example

import org.jetbrains.kotlinx.spark.api.withSpark

fun main() {
    val postgresSparkConfig = PostgresSparkConfig.fromEnv()
    val clickhouseSparkConfig = ClickhouseSparkConfig.fromEnv()

    val postgresConfig = PostgresSettings.fromEnv()
    val clickhouseConfig = ClickHouseSettings.fromEnv()

    println(postgresConfig)
    println(clickhouseConfig)

    println(postgresSparkConfig)
    println(clickhouseSparkConfig)

    withSpark {
        val postgresImporter = PostgresImporter(postgresConfig, postgresSparkConfig, spark)
        val clickhouseExporter = ClickhouseExporter(clickhouseConfig, postgresConfig, clickhouseSparkConfig, spark)

        postgresImporter.process()
        clickhouseExporter.process()
    }
}