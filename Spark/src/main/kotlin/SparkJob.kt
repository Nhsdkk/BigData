package org.example

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Dataset
import org.jetbrains.kotlinx.spark.api.SparkSession

open class SparkConfig (
    val appName: String,
    val master: String = "local"
) {
    override fun toString(): String {
        return "APP NAME: $appName\nMASTER: $master\n"
    }
}

abstract class SparkJob(
    sourceConfig: ISettings,
    targetConfig: ISettings,
    sparkConfig: SparkConfig,
    session: SparkSession) {
    private val targetConfiguration = targetConfig
    private val sourceConfiguration = sourceConfig
    private val applicationName = sparkConfig.appName

    protected val sparkSession = session

    protected abstract fun runJob()

    fun process() {
        println("Starting $applicationName job")
        runJob()
        println("Successfully finished $applicationName job")
    }

    protected fun <T> Dataset<T>.prepareWrite() : DataFrameWriter<T> {
        return this
            .write()
            .enrichWithConnection(targetConfiguration)
    }

    protected fun SparkSession.prepareRead() : DataFrameReader {
        return this
            .read()
            .enrichWithConnection(sourceConfiguration)
    }
}