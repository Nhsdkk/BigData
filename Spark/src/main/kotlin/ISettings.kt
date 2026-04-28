package org.example

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.SaveMode

interface ISettings {
    val connectionUrl: String
    val username: String
    val password: String
    val driverClassName: String
}

class ClickHouseSettings(
    override val connectionUrl: String,
    override val username: String,
    override val password: String,
    override val driverClassName: String
) : ISettings {
    companion object {
        fun fromEnv() : ClickHouseSettings {
            return ClickHouseSettings(
                connectionUrl = System.getenv("CLICKHOUSE_URL"),
                username = System.getenv("CLICKHOUSE_USERNAME"),
                password = System.getenv("CLICKHOUSE_PASSWORD"),
                driverClassName = System.getenv("CLICKHOUSE_DRIVER")
            )
        }
    }

    override fun toString(): String {
        return "CONNECTION URL:$connectionUrl\nUSERNAME: $username\nPASSWORD: $password\n DRIVER: $driverClassName\n"
    }
}

class PostgresSettings(
    override val connectionUrl: String,
    override val username: String,
    override val password: String,
    override val driverClassName: String,
    val batchSize: Int = 1000,
) : ISettings {
    companion object {
        fun fromEnv() : PostgresSettings {
            return PostgresSettings(
                connectionUrl = System.getenv("POSTGRES_URL"),
                username = System.getenv("POSTGRES_USERNAME"),
                password = System.getenv("POSTGRES_PASSWORD"),
                driverClassName = System.getenv("POSTGRES_DRIVER"),
                batchSize = System.getenv("POSTGRES_BATCH_SIZE").toIntOrNull() ?: 1000,
            )
        }
    }

    override fun toString(): String {
        return "CONNECTION URL:$connectionUrl\nUSERNAME: $username\nPASSWORD: $password\n DRIVER: $driverClassName\nBATCH_SIZE: $batchSize\n"
    }
}

fun <T> DataFrameWriter<T>.enrichWithConnection(configuration: ISettings): DataFrameWriter<T> {
    return this
        .format("jdbc")
        .option("url", configuration.connectionUrl)
        .option("user", configuration.username)
        .option("password", configuration.password)
        .option("driver", configuration.driverClassName)
}

fun DataFrameReader.enrichWithConnection(configuration: ISettings): DataFrameReader {
    return this
        .format("jdbc")
        .option("url", configuration.connectionUrl)
        .option("user", configuration.username)
        .option("password", configuration.password)
        .option("driver", configuration.driverClassName)
}


fun <T> DataFrameWriter<T>.createClickhouseView(viewName: String) {
    return this
        .mode(SaveMode.Overwrite)
        .option("dbtable", viewName)
        .saveAsTable(viewName)
}

fun <T> DataFrameWriter<T>.appendToPostgresTable(tableName: String) {
    return this
        .mode(SaveMode.Append)
        .option("dbtable", tableName)
        .save()
}

fun DataFrameReader.readFromPostgresTable(tableName: String): DataFrameReader {
    return this.option("dbtable", tableName)
}