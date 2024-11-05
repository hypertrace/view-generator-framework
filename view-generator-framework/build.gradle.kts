plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.avro-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.80")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.5.2")
  implementation("org.hypertrace.core.kafkastreams.framework:avro-partitioners:0.5.2")
  implementation("org.hypertrace.core.kafkastreams.framework:weighted-group-partitioner:0.5.2")
  implementation("org.apache.avro:avro")
  implementation("com.typesafe:config:1.4.2")
  implementation("com.google.guava:guava:32.1.3-jre")

  // Logging
  implementation("org.slf4j:slf4j-api:2.0.7")

  testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
  testImplementation("org.junit-pioneer:junit-pioneer:2.0.0")
  testImplementation("org.mockito:mockito-core:5.12.0")
  testImplementation("org.apache.kafka:kafka-streams-test-utils")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.20.0")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  setAgainstFiles(null)
}
