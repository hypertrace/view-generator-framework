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
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.33")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.26-SNAPSHOT")
  implementation("org.apache.avro:avro:1.10.2")
  implementation("com.typesafe:config:1.4.2")
  implementation("com.google.guava:guava:31.1-jre")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.36")

  constraints {
    implementation("org.glassfish.jersey.core:jersey-common:2.34") {
      because("Information Disclosure [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637] in org.glassfish.jersey.core:jersey-common@2.30")
    }
    implementation("org.apache.commons:commons-compress:1.21") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGAPACHECOMMONS-1316640] in org.apache.commons:commons-compress@1.20")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.1") {
      because("Denial of Service (DoS) [High Severity]" +
          "[https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2421244] " +
          "in com.fasterxml.jackson.core:jackson-databind@2.13.1")
    }
  }

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
  testImplementation("org.junit-pioneer:junit-pioneer:1.7.0")
  testImplementation("org.mockito:mockito-core:4.5.1")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:6.0.1-ccs")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  setAgainstFiles(null)
}
repositories {
  mavenCentral()
}
