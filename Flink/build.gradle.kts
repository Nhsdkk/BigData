plugins {
    kotlin("jvm") version "2.3.10"
    kotlin("plugin.lombok") version "2.3.20"
    kotlin("plugin.serialization") version "2.3.0"
    id("io.freefair.lombok") version "9.2.0"
    id("com.gradleup.shadow") version "9.2.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

var flinkVersion = "1.20.3"

dependencies {
    testImplementation(kotlin("test"))

    implementation("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:4.0.1-2.0")
    implementation("org.apache.flink:flink-connector-jdbc:3.3.0-1.20")
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.7.1")

    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    implementation("org.postgresql:postgresql:42.6.0")

    implementation("org.apache.flink:flink-json:$flinkVersion")
}

kotlin {
    jvmToolchain(11)
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    isZip64 = true
    mergeServiceFiles()
}