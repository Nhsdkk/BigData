package org.example

import kotlinx.datetime.LocalDate
import kotlinx.datetime.toJavaLocalDate
import kotlinx.datetime.toKotlinLocalDate
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.JdbcConnectionOptions
import org.apache.flink.connector.jdbc.JdbcExecutionOptions
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink
import org.apache.flink.connector.jdbc.datasource.statements.SimpleJdbcQueryStatement
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.time.format.DateTimeFormatter

class DateSerializer() : KSerializer<LocalDate> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("FlexibleLocalDate", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): LocalDate {
        val raw = decoder.decodeString().trim()
        for (formatter in formatters) {
            try {
                return java.time.LocalDate.parse(raw, formatter).toKotlinLocalDate()
            } catch (_: Exception) {
                continue
            }
        }

        throw SerializationException("Could not parse $raw as LocalDate")
    }

    override fun serialize(encoder: Encoder, value: LocalDate) {
        encoder.encodeString(value.toString())
    }

    private val formatters = listOf(
        DateTimeFormatter.ofPattern("M/d/yyyy"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy"))
}

@Serializable
data class MockData(
    @SerialName("id")
    val id: Int,
    @SerialName("customer_first_name")
    val customerFirstName: String,
    @SerialName("customer_last_name")
    val customerLastName: String,
    @SerialName("customer_age")
    val customerAge: Int,
    @SerialName("customer_email")
    val customerEmail: String,
    @SerialName("customer_country")
    val customerCountry: String,
    @SerialName("customer_postal_code")
    val customerPostalCode: String?,
    @SerialName("customer_pet_type")
    val customerPetType: String,
    @SerialName("customer_pet_name")
    val customerPetName: String,
    @SerialName("customer_pet_breed")
    val customerPetBreed: String,
    @SerialName("seller_first_name")
    val sellerFirstName: String,
    @SerialName("seller_last_name")
    val sellerLastName: String,
    @SerialName("seller_email")
    val sellerEmail: String,
    @SerialName("seller_country")
    val sellerCountry: String,
    @SerialName("seller_postal_code")
    val sellerPostalCode: String?,
    @SerialName("product_name")
    val productName: String,
    @SerialName("product_category")
    val productCategory: String,
    @SerialName("product_price")
    val productPrice: Double,
    @SerialName("product_quantity")
    val productQuantity: Int,
    @Serializable(with = DateSerializer::class)
    @SerialName("sale_date")
    val saleDate: LocalDate,
    @SerialName("sale_customer_id")
    val saleCustomerId: Int,
    @SerialName("sale_seller_id")
    val saleSellerId: Int,
    @SerialName("sale_product_id")
    val saleProductId: Int,
    @SerialName("sale_quantity")
    val saleQuantity: Int,
    @SerialName("sale_total_price")
    val saleTotalPrice: Double,
    @SerialName("store_name")
    val storeName: String,
    @SerialName("store_location")
    val storeLocation: String,
    @SerialName("store_city")
    val storeCity: String,
    @SerialName("store_state")
    val storeState: String?,
    @SerialName("store_country")
    val storeCountry: String,
    @SerialName("store_phone")
    val storePhone: String,
    @SerialName("store_email")
    val storeEmail: String,
    @SerialName("pet_category")
    val petCategory: String,
    @SerialName("product_weight")
    val productWeight: Double,
    @SerialName("product_color")
    val productColor: String,
    @SerialName("product_size")
    val productSize: String,
    @SerialName("product_brand")
    val productBrand: String,
    @SerialName("product_material")
    val productMaterial: String,
    @SerialName("product_description")
    val productDescription: String,
    @SerialName("product_rating")
    val productRating: Double,
    @SerialName("product_reviews")
    val productReviews: Int,
    @Serializable(with = DateSerializer::class)
    @SerialName("product_release_date")
    val productReleaseDate: LocalDate,
    @Serializable(with = DateSerializer::class)
    @SerialName("product_expiry_date")
    val productExpiryDate: LocalDate,
    @SerialName("supplier_name")
    val supplierName: String,
    @SerialName("supplier_contact")
    val supplierContact: String,
    @SerialName("supplier_email")
    val supplierEmail: String,
    @SerialName("supplier_phone")
    val supplierPhone: String,
    @SerialName("supplier_address")
    val supplierAddress: String,
    @SerialName("supplier_city")
    val supplierCity: String,
    @SerialName("supplier_country")
    val supplierCountry: String,
)

@Serializable
data class Pet(
    val id: Int,
    val name: String,
    val breed: String,
    val type: String
) {
    companion object {
        val queryStatement = SimpleJdbcQueryStatement(
            "INSERT INTO customer_pets_dim (id, name, breed, type) VALUES (?, ?, ?, ?)",
            JdbcStatementBuilder<Pet> { ps, pet ->
                ps.setInt(1, pet.id)
                ps.setString(2, pet.name)
                ps.setString(3, pet.breed)
                ps.setString(4, pet.type)
            }
        )

        fun fromMockData(mockData: CsvData): Pet {
            return Pet(
                id = mockData.rowIndex,
                name = mockData.row.customerPetName,
                breed = mockData.row.customerPetBreed,
                type = mockData.row.customerPetType,
            )
        }
    }
}

@Serializable
data class Customer(
    val id: Int,
    val firstName: String,
    val lastName: String,
    val email: String,
    val postalCode: String?,
    val country: String,
    val age: Int,
    val petId: Int
) {
    companion object {
        val queryStatement = SimpleJdbcQueryStatement(
            "INSERT INTO customers_dim(id, first_name, last_name, email, postal_code, country, age, pet_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            JdbcStatementBuilder<Customer> { ps, customer ->
                ps.setInt(1, customer.id)
                ps.setString(2, customer.firstName)
                ps.setString(3, customer.lastName)
                ps.setString(4, customer.email)
                ps.setString(5, customer.postalCode)
                ps.setString(6, customer.country)
                ps.setInt(7, customer.age)
                ps.setInt(8, customer.petId)
            }
        )

        fun fromMockData(mockData: CsvData): Customer {
            return Customer(
                id = mockData.rowIndex,
                firstName = mockData.row.customerFirstName,
                lastName = mockData.row.customerLastName,
                email = mockData.row.customerEmail,
                postalCode = mockData.row.customerPostalCode,
                country = mockData.row.customerCountry,
                age = mockData.row.customerAge,
                petId = mockData.rowIndex
            )
        }
    }
}

@Serializable
data class Seller(
    val id: Int,
    val firstName: String,
    val lastName: String,
    val email: String,
    val postalCode: String?,
    val country: String,
) {
    companion object {
        val queryStatement = SimpleJdbcQueryStatement(
            """
                INSERT INTO sellers_dim
                (id, first_name, last_name, email, postal_code, country)
                VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            JdbcStatementBuilder<Seller> { ps, seller ->
                ps.setInt(1, seller.id)
                ps.setString(2, seller.firstName)
                ps.setString(3, seller.lastName)
                ps.setString(4, seller.email)
                ps.setString(5, seller.postalCode)
                ps.setString(6, seller.country)
            }
        )

        fun fromMockData(mockData: CsvData): Seller {
            return Seller(
                id = mockData.rowIndex,
                firstName = mockData.row.sellerFirstName,
                lastName = mockData.row.sellerLastName,
                email = mockData.row.sellerEmail,
                postalCode = mockData.row.sellerPostalCode,
                country = mockData.row.sellerCountry,
            )
        }
    }
}

@Serializable
data class Supplier(
    val id: Int,
    val name: String,
    val contact: String,
    val email: String,
    val phone: String,
    val address: String,
    val city: String,
    val country: String,
) {
    companion object {
        val queryStatement = SimpleJdbcQueryStatement(
            """
                INSERT INTO suppliers_dim
                (id, name, contact, email, phone, address, city, country)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            JdbcStatementBuilder<Supplier> { ps, supplier ->
                ps.setInt(1, supplier.id)
                ps.setString(2, supplier.name)
                ps.setString(3, supplier.contact)
                ps.setString(4, supplier.email)
                ps.setString(5, supplier.phone)
                ps.setString(6, supplier.address)
                ps.setString(7, supplier.city)
                ps.setString(8, supplier.country)
            }
        )

        fun fromMockData(mockData: CsvData): Supplier {
            return Supplier(
                id = mockData.rowIndex,
                name = mockData.row.supplierName,
                contact = mockData.row.supplierContact,
                email = mockData.row.supplierEmail,
                phone = mockData.row.supplierPhone,
                address = mockData.row.supplierAddress,
                city = mockData.row.supplierCity,
                country = mockData.row.supplierCountry,
            )
        }
    }
}

@Serializable
data class Store(
    val id: Int,
    val city: String,
    val country: String,
    val email: String,
    val location: String,
    val name: String,
    val phone: String,
    val state: String?,
) {
    companion object {
        val queryStatement = SimpleJdbcQueryStatement(
            """
                INSERT INTO stores_dim
                (id, city, country, email, location, name, phone, state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            JdbcStatementBuilder<Store> { ps, store ->
                ps.setInt(1, store.id)
                ps.setString(2, store.city)
                ps.setString(3, store.country)
                ps.setString(4, store.email)
                ps.setString(5, store.location)
                ps.setString(6, store.name)
                ps.setString(7, store.phone)
                ps.setString(8, store.state)
            }
        )

        fun fromMockData(mockData: CsvData): Store {
            return Store(
                id = mockData.rowIndex,
                city = mockData.row.storeCity,
                country = mockData.row.storeCountry,
                email = mockData.row.storeEmail,
                location = mockData.row.storeLocation,
                name = mockData.row.storeName,
                phone = mockData.row.storePhone,
                state = mockData.row.storeState,
            )
        }
    }
}

@Serializable
data class Product(
    val id: Int,
    val weight: Double,
    val color: String,
    val size: String,
    val brand: String,
    val material: String,
    val description: String,
    val rating: Double,
    val reviews: Int,
    val releaseDate: LocalDate,
    val expiryDate: LocalDate,
    val category: String,
    val name: String,
    val price: Double,
    val quantity: Int,
) {
    companion object {
        val queryStatement = SimpleJdbcQueryStatement(
            """
                INSERT INTO products_dim
                (weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date, category, name, price, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            JdbcStatementBuilder<Product> { ps, product ->
//                ps.setInt(1, product.id)
                ps.setDouble(1, product.weight)
                ps.setString(2, product.color)
                ps.setString(3, product.size)
                ps.setString(4, product.brand)
                ps.setString(5, product.material)
                ps.setString(6, product.description)
                ps.setDouble(7, product.rating)
                ps.setInt(8, product.reviews)
                ps.setDate(9, java.sql.Date.valueOf(product.releaseDate.toJavaLocalDate()))
                ps.setDate(10, java.sql.Date.valueOf(product.expiryDate.toJavaLocalDate()))
                ps.setString(11, product.category)
                ps.setString(12, product.name)
                ps.setDouble(13, product.price)
                ps.setInt(14, product.quantity)
            }
        )

        fun fromMockData(mockData: CsvData): Product {
            return Product(
                id = mockData.rowIndex,
                weight = mockData.row.productWeight,
                color = mockData.row.productColor,
                size = mockData.row.productSize,
                brand = mockData.row.productBrand,
                material = mockData.row.productMaterial,
                description = mockData.row.productDescription,
                rating = mockData.row.productRating,
                reviews = mockData.row.productReviews,
                releaseDate = mockData.row.productReleaseDate,
                expiryDate = mockData.row.productExpiryDate,
                category = mockData.row.productCategory,
                name = mockData.row.productName,
                price = mockData.row.productPrice,
                quantity = mockData.row.productQuantity,
            )
        }
    }
}

@Serializable
data class Transaction(
    val id: Int,
    val customerId: Int,
    val saleDate: LocalDate,
    val productId: Int,
    val quantity: Int,
    val sellerId: Int,
    val storeId: Int,
    val totalPrice: Double,
    val petCategory: String,
    val supplierId: Int,
) {
    companion object {
        val queryStatement = SimpleJdbcQueryStatement(
            """
                INSERT INTO transactions_fact
                (customer_id, date, product_id, quantity, seller_id, store_id, total_price, pet_category, supplier_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            JdbcStatementBuilder<Transaction> { ps, t ->
//                ps.setInt(1, t.id)
                ps.setInt(1, t.customerId)
                ps.setDate(2, java.sql.Date.valueOf(t.saleDate.toJavaLocalDate()))
                ps.setInt(3, t.productId)
                ps.setInt(4, t.quantity)
                ps.setInt(5, t.sellerId)
                ps.setInt(6, t.storeId)
                ps.setDouble(7, t.totalPrice)
                ps.setString(8, t.petCategory)
                ps.setInt(9, t.supplierId)
            }
        )

        private fun recalculateIndex(referenceIndex: Int, index: Int, batchSize: Int): Int {
            return (referenceIndex - 1) / batchSize * batchSize + index
        }

        fun fromMockData(mockData: CsvData, batchSize: Int): Transaction {
            println("ID: ${mockData.rowIndex}")
            return Transaction(
                id = mockData.rowIndex,
                customerId = recalculateIndex(mockData.rowIndex, mockData.row.saleCustomerId, batchSize),
                saleDate = mockData.row.saleDate,
                productId = recalculateIndex(mockData.rowIndex, mockData.row.saleProductId, batchSize),
                quantity = mockData.row.saleQuantity,
                sellerId = recalculateIndex(mockData.rowIndex, mockData.row.saleSellerId, batchSize),
                storeId = mockData.rowIndex,
                totalPrice = mockData.row.saleTotalPrice,
                petCategory = mockData.row.petCategory,
                supplierId = mockData.rowIndex,
            )
        }
    }
}

class KafkaProcessor(
    kafkaSourceConfig: KafkaConfig,
    private val postgresSinkConfig: PostgresConfig,
) {
    private val kafkaSource = KafkaSource.builder<String>()
        .setBootstrapServers(kafkaSourceConfig.bootstrapServers)
        .setTopics(kafkaSourceConfig.topicName)
        .setGroupId("csv_processor_group")
        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()

    private val petSink = JdbcSink.builder<Pet>()
        .withExecutionOptions(JdbcExecutionOptions.defaults())
        .withQueryStatement(Pet.queryStatement)
        .buildAtLeastOnce(postgresSinkConfig.getConnectionOptions())

    private val customersSink = JdbcSink.builder<Customer>()
        .withQueryStatement(Customer.queryStatement)
        .buildAtLeastOnce(postgresSinkConfig.getConnectionOptions())

    private val productsSink = JdbcSink.builder<Product>()
        .withQueryStatement(Product.queryStatement)
        .buildAtLeastOnce(postgresSinkConfig.getConnectionOptions())

    private val sellersSink = JdbcSink.builder<Seller>()
        .withQueryStatement(Seller.queryStatement)
        .buildAtLeastOnce(postgresSinkConfig.getConnectionOptions())

    private val storeSink = JdbcSink.builder<Store>()
        .withQueryStatement(Store.queryStatement)
        .buildAtLeastOnce(postgresSinkConfig.getConnectionOptions())

    private val transactionSink = JdbcSink.builder<Transaction>()
        .withQueryStatement(Transaction.queryStatement)
        .buildAtLeastOnce(postgresSinkConfig.getConnectionOptions())

    private val supplierSink = JdbcSink.builder<Supplier>()
        .withQueryStatement(Supplier.queryStatement)
        .buildAtLeastOnce(postgresSinkConfig.getConnectionOptions())

    private val env = StreamExecutionEnvironment.getExecutionEnvironment()

    private class TransactionMapper(private val batchSize: Int) : MapFunction<CsvData, Transaction> {
        override fun map(value: CsvData): Transaction = Transaction.fromMockData(value, batchSize)
    }

    fun process() {
        val data = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .map { Json.decodeFromString<CsvData>(it) }

        data.map { Pet.fromMockData(it) }.sinkTo(petSink)
        data.map { Customer.fromMockData(it) }.sinkTo(customersSink)
        data.map { Product.fromMockData(it) }.sinkTo(productsSink)
        data.map { Seller.fromMockData(it) }.sinkTo(sellersSink)
        data.map { Store.fromMockData(it) }.sinkTo(storeSink)
        data.map { Supplier.fromMockData(it) }.sinkTo(supplierSink)
        data.map(TransactionMapper(postgresSinkConfig.batchSize)).sinkTo(transactionSink)

        env.execute()
    }
}

@Serializable
data class CsvData(
    val rowIndex: Int,
    val row: MockData
)

data class KafkaConfig(
    val topicName: String,
    val bootstrapServers: String,
) {
    companion object {
        fun fromEnv() : KafkaConfig = KafkaConfig(
            topicName = System.getenv("KAFKA_TOPIC_NAME"),
            bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        )
    }
}

data class PostgresConfig(
    val connectionUrl: String,
    val database: String,
    val username: String,
    val password: String,
    val batchSize: Int
) {
    companion object {
        fun fromEnv() : PostgresConfig {
            return PostgresConfig(
                connectionUrl = System.getenv("POSTGRES_URL"),
                database = System.getenv("POSTGRES_DATABASE"),
                username = System.getenv("POSTGRES_USERNAME"),
                password = System.getenv("POSTGRES_PASSWORD"),
                batchSize = System.getenv("POSTGRES_BATCH_SIZE").toIntOrNull() ?: 1000,
            )
        }
    }

    fun getConnectionOptions(): JdbcConnectionOptions {
        return JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(connectionUrl)
            .withUsername(username)
            .withPassword(password)
            .withDriverName("org.postgresql.Driver")
            .build()
    }
}

fun main() {
//    val kafkaConfig = KafkaConfig(
//        "csv_topic",
//        "localhost:9093",
//    )

    val kafkaConfig = KafkaConfig.fromEnv()

//    val postgresConfig = PostgresConfig(
//        "jdbc:postgresql://localhost:5432/postgres",
//        "postgres",
//        "postgres",
//        "postgres",
//        1000
//    )

    val postgresConfig = PostgresConfig.fromEnv()

    KafkaProcessor(kafkaConfig, postgresConfig).process()
}