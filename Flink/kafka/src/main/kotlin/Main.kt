package org.example

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


data class KafkaConfig(
    val topicName: String,
    val bootstrapServers: String,
) {
    companion object {
        fun fromEnv(): KafkaConfig = KafkaConfig(
            topicName = System.getenv("KAFKA_TOPIC_NAME"),
            bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        )
    }
}

data class CsvProcessorConfig(
    val dataFolderPath: String
) {
    companion object {
        fun fromEnv(): CsvProcessorConfig = CsvProcessorConfig(
            dataFolderPath = System.getenv("CSV_DATA_FOLDER_PATH")
        )
    }
}

@Serializable
data class CsvData(
    val rowIndex: Int,
    val row: Map<String, String>,
)

class CsvProcessor(
    private val processorConfig: CsvProcessorConfig,
    private val kafkaConfig: KafkaConfig,
) : AutoCloseable {
    private val producer = KafkaProducer<String, ByteArray>(
        mapOf<String, String>(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to kafkaConfig.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "PLAINTEXT",
        )
    )

    private fun getFilePaths(folderPath: String): List<Path> {
        val dataDirPath = Paths.get(folderPath)

        return Files.walk(dataDirPath)
            .filter { Files.isRegularFile(it) }
            .filter { it.toString().endsWith(".csv") }
            .toList()
    }

    private fun sendToKafka(file: File, fileIndex: Int) {
        csvReader().open(file) {
            readAllWithHeaderAsSequence()
                .mapIndexed { index, it ->
                    ProducerRecord(
                        kafkaConfig.topicName,
                        (fileIndex * 1000 + index + 1).toString(),
                        Json.encodeToString(
                            CsvData(
                                rowIndex = fileIndex * 1000 + index + 1,
                                row = it
                            )
                        ).toByteArray(),
                    )
                }
                .forEach {
                    println("Sending record with id = ${it.key()}")
                    producer.send(it)
                }
        }
    }

    fun process() {
        println("Searching for files in ${processorConfig.dataFolderPath}")
        val filePaths = getFilePaths(processorConfig.dataFolderPath)

        println("Found ${filePaths.size} file paths. Starting processing...")
        for ((fileIndex, filePath) in filePaths.sortedBy { it.toString() }.withIndex()) {
            println("Processing $filePath...")
            sendToKafka(filePath.toFile(), fileIndex)
            println("Successfully processed $filePath")
        }
    }

    override fun close() {
        producer.close()
    }

}

fun main() {
//    val kafkaConfig = KafkaConfig(
//        "csv_topic",
//        "localhost:9093",
//    )

    val kafkaConfig = KafkaConfig.fromEnv()

//    val csvProcessorConfig = CsvProcessorConfig("./data")
    val csvProcessorConfig = CsvProcessorConfig.fromEnv()
    CsvProcessor(csvProcessorConfig, kafkaConfig).use {
        it.process()
    }
}