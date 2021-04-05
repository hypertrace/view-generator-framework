plugins {
  `java-library`
  jacoco
  id("com.commercehub.gradle.plugin.avro") version "0.9.1"
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}


sourceSets {
  test {
    java {
      srcDir("build/generated-test-avro-java")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.23")

  implementation("org.apache.avro:avro:1.10.2")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")

  implementation("com.typesafe:config:1.4.1")

  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.20")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-serdes:0.1.20")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")

  constraints {

  }
}
