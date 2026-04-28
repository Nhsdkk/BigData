plugins {
    kotlin("jvm") version "2.3.10"
    id("com.gradleup.shadow") version "9.2.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    // Kotlin Spark API
    implementation("org.jetbrains.kotlinx.spark:kotlin-spark-api_3.3.2_2.13:1.2.4")
    // Apache Spark
    compileOnly("org.apache.spark:spark-sql_2.13:3.3.2")

    implementation("org.postgresql:postgresql:42.7.10")

    implementation("com.clickhouse:clickhouse-jdbc:0.7.2:shaded-all")
}

kotlin {
    jvmToolchain(17)
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    isZip64 = true
    mergeServiceFiles()
}
