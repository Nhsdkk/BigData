plugins {
    kotlin("jvm")
    id("com.gradleup.shadow") version "9.2.0"
    kotlin("plugin.serialization") version "2.3.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation("org.slf4j:slf4j-api:1.7.36")
    runtimeOnly("org.slf4j:slf4j-simple:1.7.36")

    implementation("com.jsoizo:kotlin-csv-jvm:1.10.0")

    implementation("org.apache.kafka:kafka-clients:3.8.0")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.21.2")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
}

kotlin {
    jvmToolchain(24)
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    isZip64 = true
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "org.example.MainKt"
    }
}
