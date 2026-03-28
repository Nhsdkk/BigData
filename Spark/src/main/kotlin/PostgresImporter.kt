package org.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType

class PostgresSparkConfig(
    master: String = "local",
) : SparkConfig(
    "Postgres importer job",
    master,
) {
    companion object {
        fun fromEnv(): PostgresSparkConfig {
            return PostgresSparkConfig(
                master = System.getenv("SPARK_MASTER"),
            )
        }
    }
}

class PostgresImporter(
    postgresSettings: PostgresSettings,
    jobConfig: PostgresSparkConfig,
    session: SparkSession,
) : SparkJob(postgresSettings, postgresSettings, jobConfig, session) {
    private val batchSize = postgresSettings.batchSize

    private val mockData = sparkSession.loadDataFromPostgresTable("mock_data")

    private fun loadPets() {
        mockData
            .select(
                col("customer_pet_inner_id").alias("id"),
                col("customer_pet_name").alias("name"),
                col("customer_pet_breed").alias("breed"),
                col("customer_pet_type").alias("type")
            )
            .loadDataIntoPostgresTable("customer_pets_dim")
    }

    private fun loadCustomers() {
        mockData
            .select(
                col("id"),
                col("customer_first_name").alias("first_name"),
                col("customer_last_name").alias("last_name"),
                col("customer_email").alias("email"),
                col("customer_postal_code").alias("postal_code"),
                col("customer_country").alias("country"),
                col("customer_age").alias("age"),
                col("customer_pet_inner_id").alias("pet_id"),
            )
            .loadDataIntoPostgresTable("customers_dim")
    }

    private fun loadSellers() {
        mockData
            .select(
                col("id"),
                col("seller_first_name").alias("first_name"),
                col("seller_last_name").alias("last_name"),
                col("seller_email").alias("email"),
                col("seller_postal_code").alias("postal_code"),
                col("seller_country").alias("country"),
            )
            .loadDataIntoPostgresTable("sellers_dim")
    }

    private fun loadSuppliers() {
        mockData
            .select(
                col("supplier_inner_id").alias("id"),
                col("supplier_name").alias("name"),
                col("supplier_contact").alias("contact"),
                col("supplier_email").alias("email"),
                col("supplier_phone").alias("phone"),
                col("supplier_address").alias("address"),
                col("supplier_city").alias("city"),
                col("supplier_country").alias("country"),
            )
            .loadDataIntoPostgresTable("suppliers_dim")
    }

    private fun loadStores() {
        mockData
            .select(
                col("store_inner_id").alias("id"),
                col("store_city").alias("city"),
                col("store_country").alias("country"),
                col("store_email").alias("email"),
                col("store_location").alias("location"),
                col("store_name").alias("name"),
                col("store_phone").alias("phone"),
                col("store_state").alias("state"),
            )
            .loadDataIntoPostgresTable("stores_dim")
    }

    private fun loadProducts() {
        mockData
            .select(
                col("product_inner_id").alias("id"),
                col("product_weight").alias("weight"),
                col("product_color").alias("color"),
                col("product_size").alias("size"),
                col("product_brand").alias("brand"),
                col("product_material").alias("material"),
                col("product_description").alias("description"),
                col("product_rating").alias("rating"),
                col("product_reviews").alias("reviews"),
                col("product_release_date").alias("release_date"),
                col("product_expiry_date").alias("expiry_date"),
                col("product_category").alias("category"),
                col("product_name").alias("name"),
                col("product_price").alias("price"),
                col("product_quantity").alias("quantity"),
            )
            .loadDataIntoPostgresTable("products_dim")
    }

    private fun loadTransactions() {
        mockData
            .select(
                col("id"),
                col("sale_customer_id").alias("customer_id"),
                col("sale_date").alias("date"),
                col("sale_product_id").alias("product_id"),
                col("sale_quantity").alias("quantity"),
                col("sale_seller_id").alias("seller_id"),
                col("store_inner_id").alias("store_id"),
                col("sale_total_price").alias("total_price"),
                col("pet_category").alias("pet_category"),
                col("supplier_inner_id").alias("supplier_id"),
            )
            .withColumn(
                "customer_id",
                col("customer_id")
                    .plus(
                        col("id")
                            .minus(1)
                            .divide(batchSize)
                            .cast(DataType.fromDDL("integer"))
                            .multiply(batchSize)
                    )
            )
            .withColumn(
                "product_id",
                col("product_id")
                    .plus(
                        col("id")
                            .minus(1)
                            .divide(batchSize)
                            .cast(DataType.fromDDL("integer"))
                            .multiply(batchSize)
                    )
            )
            .withColumn(
                "seller_id",
                col("seller_id")
                    .plus(
                        col("id")
                            .minus(1)
                            .divide(batchSize)
                            .cast(DataType.fromDDL("integer"))
                            .multiply(batchSize)
                    )
            )
            .loadDataIntoPostgresTable("transactions_fact")
    }

    private fun SparkSession.loadDataFromPostgresTable(tableName: String) : Dataset<Row> {
        return this
            .prepareRead()
            .readFromPostgresTable(tableName)
            .load()
    }

    private fun Dataset<Row>.loadDataIntoPostgresTable(tableName: String) {
        return this
            .prepareWrite()
            .appendToPostgresTable(tableName)
    }

    override fun runJob() {
        loadPets()
        loadCustomers()
        loadSellers()
        loadSuppliers()
        loadStores()
        loadProducts()
        loadTransactions()
    }
}