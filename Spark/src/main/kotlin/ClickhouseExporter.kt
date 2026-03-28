package org.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.jetbrains.kotlinx.spark.api.SparkSession
import org.jetbrains.kotlinx.spark.api.col
import org.jetbrains.kotlinx.spark.api.lit

class ClickhouseSparkConfig(
    master: String = "local",
) : SparkConfig(
    "Clickhouse exporter job",
    master
) {
    companion object {
        fun fromEnv(): ClickhouseSparkConfig {
            return ClickhouseSparkConfig(
                master = System.getenv("SPARK_MASTER"),
            )
        }
    }
}

class ClickhouseExporter(
    clickhouseConfig: ClickHouseSettings,
    postgresConfig: PostgresSettings,
    jobConfig: ClickhouseSparkConfig,
    session: SparkSession
) : SparkJob(postgresConfig, clickhouseConfig, jobConfig, session) {
    private val products = sparkSession.loadDataFromPostgresTable("products_dim")
    private val transactions = sparkSession.loadDataFromPostgresTable("transactions_fact")
    private val customers = sparkSession.loadDataFromPostgresTable("customers_dim")
    private val suppliers = sparkSession.loadDataFromPostgresTable("suppliers_dim")
    private val stores = sparkSession.loadDataFromPostgresTable("stores_dim")

    override fun runJob() {
        exportProducts()
        exportCustomers()
        exportTransactions()
        exportStores()
        exportSuppliers()
        exportProductQuality()
    }

    private fun Dataset<Row>.perMonthAndYearStatistics(name: String, columnName: String): Dataset<Row> {
        val prevYearSameMonthWindow = Window.partitionBy(
            org.jetbrains.kotlinx.spark.api.col("month"),
        ).orderBy(
            org.jetbrains.kotlinx.spark.api.col("year"),
        )

        val prevMonthWindow = Window.orderBy(
            org.jetbrains.kotlinx.spark.api.col("year"),
            org.jetbrains.kotlinx.spark.api.col("month"),
        )

        val prevMonthValue = functions.lag(org.jetbrains.kotlinx.spark.api.col(columnName), 1).over(prevMonthWindow)
        val prevYearSameMonthValue = functions.lag(org.jetbrains.kotlinx.spark.api.col(columnName), 1).over(prevYearSameMonthWindow)

        return this
            .withColumn(
                "prev_month_${name}",
                functions.coalesce(
                    prevMonthValue,
                    lit(0)
                )
            )
            .withColumn(
                "percent_diff_prev_month_${name}",
                functions.`when`(
                    prevMonthValue.isNull.or(prevMonthValue.equalTo(lit(0.0f))),
                    lit(0.0f)
                )
                    .otherwise(
                        org.jetbrains.kotlinx.spark.api.col(columnName)
                            .minus(functions.lag(org.jetbrains.kotlinx.spark.api.col(columnName), 1).over(prevMonthWindow))
                            .divide(functions.lag(org.jetbrains.kotlinx.spark.api.col(columnName), 1).over(prevMonthWindow))
                            .multiply(lit(100))
                    )
            )
            .withColumn(
                "prev_year_same_month_${name}",
                functions.coalesce(
                    prevYearSameMonthValue,
                    lit(0)
                )
            )
            .withColumn(
                "percent_diff_prev_year_same_month_${name}",
                functions.`when`(
                    prevYearSameMonthValue.isNull.or(prevYearSameMonthValue.equalTo(lit(0.0f))),
                    lit(0.0f)
                )
                    .otherwise(
                        org.jetbrains.kotlinx.spark.api.col(columnName)
                            .minus(functions.lag(org.jetbrains.kotlinx.spark.api.col(columnName), 1).over(prevYearSameMonthWindow))
                            .divide(functions.lag(org.jetbrains.kotlinx.spark.api.col(columnName), 1).over(prevYearSameMonthWindow))
                            .multiply(lit(100))
                    )
            )
    }

    private fun exportTransactions() {
        transactions
            .withColumn(
                "month",
                functions.month(col("date"))
            )
            .withColumn(
                "year",
                functions.year(col("date"))
            )
            .groupBy(
                col("month"),
                col("year")
            )
            .agg(
                functions.sum(transactions.col("total_price")).alias("total_income_per_month"),
                functions.count(transactions.col("id")).alias("total_transactions_per_month"),
                functions.sum(transactions.col("quantity")).alias("total_sold_items_per_month"),
                functions.avg(transactions.col("quantity")).alias("average_sold_items_per_month"),
            )
            .perMonthAndYearStatistics("income", "total_income_per_month")
            .perMonthAndYearStatistics("transactions_count", "total_transactions_per_month")
            .perMonthAndYearStatistics("sold_items_count", "total_sold_items_per_month")
            .select(
                col("month"),
                col("year"),
                col("average_sold_items_per_month"),
                col("total_sold_items_per_month"),
                col("total_transactions_per_month"),
                col("total_income_per_month"),
                *listOf("income", "transactions_count", "sold_items_count").flatMap {
                    listOf(
                        col("prev_month_${it}"),
                        col("percent_diff_prev_month_${it}"),
                        col("prev_year_same_month_${it}"),
                        col("percent_diff_prev_year_same_month_${it}"),
                    )
                }.toTypedArray()
            )
            .loadDataIntoClickhouseTable("transactions_by_years_and_months")
    }

    private fun exportProducts() {
        val categoryWindow = Window.partitionBy(products.col("category")).orderBy(products.col("category"))
        val totalAmountSoldWindow = Window.orderBy(col("total_amount_sold").desc())

        products
            .join(
                transactions,
                products.col("id").equalTo(transactions.col("product_id")),
            )
            .withColumn(
                "rating_sum",
                products.col("rating").multiply(products.col("reviews"))
            )
            .groupBy(
                products.col("name"),
                products.col("brand"),
                products.col("category")
            )
            .agg(
                functions.sum(transactions.col("total_price")).alias("total_sell_value"),
                functions.sum(products.col("quantity")).alias("total_amount_sold"),
                functions.sum(products.col("reviews")).alias("total_review_count"),
                functions.sum(col("rating_sum")).alias("total_rating_sum")
            )
            .withColumn(
                "total_sell_value_over_category",
                functions.sum("total_sell_value").over(categoryWindow)
            )
            .withColumn(
                "average_rating",
                col("total_rating_sum").divide(col("total_review_count"))
            )
            .withColumn(
                "is_in_top_10_sold_products",
                functions.row_number().over(totalAmountSoldWindow).leq(lit(10))
            )
            .select(
                products.col("name"),
                products.col("brand"),
                products.col("category"),
                col("average_rating"),
                col("is_in_top_10_sold_products"),
                col("total_amount_sold"),
                col("total_review_count")
            )
            .loadDataIntoClickhouseTable("products_view")
    }

    private fun exportCustomers() {
        val countryWindow = Window.partitionBy(customers.col("country")).orderBy(customers.col("country"))
        val totalMoneySpentWindow = Window.orderBy(col("total_money_spent").desc())

        customers
            .join(
                transactions,
                customers.col("id")
                    .equalTo(transactions.col("customer_id"))
            )
            .groupBy(
                customers.col("id"),
                customers.col("first_name"),
                customers.col("last_name"),
                customers.col("email"),
                customers.col("country")
            )
            .agg(
                functions.sum(transactions.col("total_price")).alias("total_money_spent"),
                functions.avg(col("total_price")).alias("average_receipt_total")
            )
            .withColumn(
                "is_in_top_10_spenders",
                functions.row_number().over(totalMoneySpentWindow).leq(lit(10))
            )
            .withColumn(
                "customer_count_by_country",
                functions.count(customers.col("id")).over(countryWindow)
            )
            .select(
                customers.col("id"),
                customers.col("first_name"),
                customers.col("last_name"),
                customers.col("email"),
                customers.col("country"),
                col("is_in_top_10_spenders"),
                col("total_money_spent"),
                col("customer_count_by_country"),
                col("average_receipt_total")
            )
            .loadDataIntoClickhouseTable("customers_view")
    }

    private fun exportStores() {
        val countryWindow = Window.partitionBy(stores.col("country")).orderBy(stores.col("country"))
        val cityWindow = Window.partitionBy(stores.col("city")).orderBy(stores.col("country"))
        val totalRevenueWindow = Window.orderBy(col("total_revenue").desc())

        stores
            .join(
                transactions,
                stores.col("id").equalTo(transactions.col("store_id")),
            )
            .groupBy(
                stores.col("id"),
                stores.col("name"),
                stores.col("email"),
                stores.col("country"),
                stores.col("city"),
            )
            .agg(
                functions.sum(transactions.col("total_price")).alias("total_revenue"),
                functions.avg(transactions.col("total_price")).alias("average_receipt_total"),
                functions.count(transactions.col("id")).alias("total_transactions"),
            )
            .withColumn(
                "is_in_top_5_by_revenue",
                functions.row_number().over(totalRevenueWindow).leq(lit(5))
            )
            .withColumn(
                "transactions_count_by_country",
                functions.sum(col("total_transactions")).over(countryWindow)
            )
            .withColumn(
                "transactions_count_by_city",
                functions.sum(col("total_transactions")).over(cityWindow)
            )
            .select(
                stores.col("id"),
                stores.col("name"),
                stores.col("email"),
                stores.col("country"),
                stores.col("city"),
                col("is_in_top_5_by_revenue"),
                col("total_revenue"),
                col("transactions_count_by_country"),
                col("transactions_count_by_city"),
                col("average_receipt_total"),
            )
            .loadDataIntoClickhouseTable("stores_view")
    }

    private fun exportSuppliers() {
        val countryWindow = Window.partitionBy(suppliers.col("country")).orderBy(suppliers.col("country"))
        val totalRevenueWindow = Window.orderBy(col("total_revenue").desc())

        suppliers
            .join(
                transactions,
                suppliers.col("id")
                    .equalTo(transactions.col("supplier_id"))
            )
            .join(
                products,
                products.col("id")
                    .equalTo(transactions.col("product_id"))
            )
            .groupBy(
                suppliers.col("id"),
                suppliers.col("name"),
                suppliers.col("email"),
                suppliers.col("country"),
            )
            .agg(
                functions.sum(transactions.col("total_price")).alias("total_revenue"),
                functions.avg(products.col("price")).alias("average_product_cost"),
                functions.count(products.col("id")).alias("total_transactions"),
            )
            .withColumn(
                "is_in_top_5_by_revenue",
                functions.row_number().over(totalRevenueWindow).leq(lit(5))
            )
            .withColumn(
                "transactions_count_by_country",
                functions.sum(col("total_transactions")).over(countryWindow)
            )
            .select(
                suppliers.col("id"),
                suppliers.col("name"),
                suppliers.col("email"),
                suppliers.col("country"),
                col("is_in_top_5_by_revenue"),
                col("total_revenue"),
                col("average_product_cost"),
                col("transactions_count_by_country")
            )
            .loadDataIntoClickhouseTable("suppliers_view")
    }

    private fun exportProductQuality() {
        val p1 = products
            .join(
                transactions,
                products.col("id").equalTo(transactions.col("product_id")),
            )
            .withColumn(
                "rating_sum",
                products.col("rating").multiply(products.col("reviews"))
            )
            .groupBy(
                products.col("name"),
                products.col("brand"),
            )
            .agg(
                functions.sum(transactions.col("quantity")).alias("total_sale_count"),
                functions.sum(products.col("reviews")).alias("total_review_count"),
                functions.sum(col("rating_sum")).alias("total_rating_sum")
            )
            .withColumn(
                "average_rating",
                col("total_rating_sum").divide(col("total_review_count"))
            )

        val stats = p1.agg(
            functions.max(col("average_rating")).alias("max_rating"),
            functions.min(col("average_rating")).alias("min_rating"),
            functions.max(col("total_review_count")).alias("max_review_count"),
            functions.corr(col("average_rating"), col("total_sale_count")).alias("rating_to_sale_count_corr"),
        )

        p1
            .crossJoin(stats)
            .withColumn(
                "has_max_review_count",
                col("total_review_count").equalTo(stats.col("max_review_count"))
            )
            .withColumn(
                "has_max_average_rating",
                col("average_rating").equalTo(stats.col("max_rating"))
            )
            .withColumn(
                "has_min_average_rating",
                col("average_rating").equalTo(stats.col("min_rating"))
            )
            .select(
                products.col("name"),
                products.col("brand"),
                col("has_max_review_count"),
                col("has_max_average_rating"),
                col("has_min_average_rating"),
                stats.col("rating_to_sale_count_corr")
            )
            .loadDataIntoClickhouseTable("product_quality_view")
    }

    private fun SparkSession.loadDataFromPostgresTable(tableName: String) : Dataset<Row> {
        return this
            .prepareRead()
            .readFromPostgresTable(tableName)
            .load()
    }

    private fun Dataset<Row>.loadDataIntoClickhouseTable(tableName: String) {
        this
            .prepareWrite()
            .createClickhouseView(tableName)
    }
}