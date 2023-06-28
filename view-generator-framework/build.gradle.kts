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
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.53")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.2.13")
  implementation("org.hypertrace.core.kafkastreams.framework:avro-partitioners:0.2.13")
  implementation("org.apache.avro:avro:1.11.1")
  constraints {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2") {
      because("version 2.12.7.1 has a vulnerability https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-3038424")
    }
  }
  implementation("com.typesafe:config:1.4.2")
  implementation("com.google.guava:guava:32.0.1-jre")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.36")

  testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
  testImplementation("org.junit-pioneer:junit-pioneer:1.7.1")
  testImplementation("org.mockito:mockito-core:4.7.0")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:7.2.1-ccs")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  setAgainstFiles(null)
}
